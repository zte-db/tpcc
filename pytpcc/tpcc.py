import sys
import os
import string
import datetime
import logging
import re
import argparse
import glob
import time 
import multiprocessing
from configparser import SafeConfigParser
from pprint import pprint,pformat
from drivers.postgresqldriver import *
from util import *
from runtime import *
import drivers
import constants
import os
import sys
import traceback

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
                    
## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    drivers = [ ]
    for f in map(lambda x: os.path.basename(x).replace("driver.py", ""), glob.glob("./drivers/*driver.py")):
        if f != "abstract": drivers.append(f)
    return (drivers)
## DEF

## ==============================================
## startLoading
## ==============================================
def startLoading(driver, scaleParameters, args):
    logging.info("Creating client pool with %d processes" % args['clients'])
    pool = multiprocessing.Pool(args['clients'])

    
    # Split the warehouses into chunks
    w_ids = map(lambda x: [ ], range(args['clients']))
    for w_id in range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1):
        idx = w_id % args['clients']
        w_ids[idx].append(w_id)
    ## FOR
    
    loader_results = [ ]
    for i in range(args['clients']):
        r = pool.apply_async(loaderFunc, (driver, scaleParameters,w_ids[i]))
        loader_results.append(r)
    ## FOR
    
    pool.close()
    logging.info("Waiting for %d loaders to finish" % args['clients'])
    pool.join()
## DEF

## ==============================================
## loaderFunc
## ==============================================
def loaderFunc(driver, scaleParameters, w_ids):
    assert driver != None
    logging.info("Starting client execution: %s [warehouses=%d]" % (driver, len(w_ids)))
   
    try:
        loadItems = (1 in w_ids)
        l = loader.Loader(driver, scaleParameters, w_ids, loadItems)
        driver.loadStart()
        l.execute()
        driver.loadFinish()   
    except KeyboardInterrupt:
            return -1
    except (Exception, AssertionError) as ex:
        logging.warn("Failed to load data: %s" % (ex))
        #if debug:
        traceback.print_exc(file=sys.stdout)
        raise
        
## DEF

## ==============================================
## startExecution
## ==============================================
def startExecution(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes" % args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    
    worker_results = [ ]
    for i in range(args['clients']):
        r = pool.apply_async(executorFunc, (driverClass, scaleParameters, args, config, debug,))
        worker_results.append(r)
    ## FOR
    pool.close()
    pool.join()
    
    total_results = results.Results()
    for asyncr in worker_results:
        asyncr.wait()
        r = asyncr.get()
        assert r != None, "No results object returned!"
        if type(r) == int and r == -1: sys.exit(1)
        total_results.append(r)
    ## FOR
    
    return (total_results)
## DEF

## ==============================================
## executorFunc
## ==============================================
def executorFunc(driverClass, scaleParameters, args, config, debug):
    driver = driverClass(args['ddl'])
    assert driver != None
    logging.debug("Starting client execution: %s" % driver)
    
    config['execute'] = True
    config['reset'] = False
    driver.loadConfig(config)

    e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
    driver.executeStart()
    results = e.execute(args['duration'])
    driver.executeFinish()
    
    return results
## DEF


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('--system', choices=getDrivers(),
                         help='Target system driver')
    aparser.add_argument('--config',default=None, type=str,
                         help="Path to driver configuration file")
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=4, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--ddl', default=constants.ddl,
                         help='Path to the TPC-C DDL SQL file')
    aparser.add_argument('--clients', default=1, type=int, metavar='N',
                         help='The number of blocking clients to fork')
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the system and exit')

    args = vars(aparser.parse_args())
    config = dict(map(lambda x: (x, config.POSTGRESQL_CONFIG[x][1]), config.POSTGRESQL_CONFIG.keys()))
    for key,val in args.items():    config[key]=val


    ## Create a handle to the target client driver
    driver = PostgresqlDriver(args['ddl'])

    driver.loadConfig(config)

    ## Create ScaleParameters
    scaleParameters = scaleparameters.makeWithScaleFactor(config['warehouses'], config['scalefactor'])
    nurand = rand.setNURand(rand.makeForLoad())
    
    
    ## DATA LOADER!!!
    load_time = None
    if config['reset']:
        logging.info("Loading TPC-C benchmark data using %s" % (driver))
        load_start = time.time()
        if args['clients'] == 1:
            l = loader.Loader(driver, scaleParameters, range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1), True)
            driver.loadStart()
            l.execute()
            driver.loadFinish()
        else:
            startLoading(driver, scaleParameters, args)
        load_time = time.time() - load_start
    
    
    # # WORKLOAD DRIVER!!!
    # if not args['no_execute']:
    #     if args['clients'] == 1:
    #         e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
    #         driver.executeStart()
    #         results = e.execute(args['duration'])
    #         driver.executeFinish()
    #     else:
    #         results = startExecution(driver, scaleParameters, args, config)
    #     assert results
    #     print(results.show(load_time))
    # # IF
    
## MAIN