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
from util.config import *


logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)
                    

## ==============================================
## startLoading
## ==============================================
def startLoading(driver, scaleParameters, config):
    logging.info("Creating client pool with %d processes" % config['clients'])
    pool = multiprocessing.Pool(config['clients'])

    
    # Split the warehouses into chunks
    w_ids = map(lambda x: [ ], range(config['clients']))
    for w_id in range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1):
        idx = w_id % config['clients']
        w_ids[idx].append(w_id)
    ## FOR
    
    loader_results = [ ]
    for i in range(config['clients']):
        r = pool.apply_async(loaderFunc, (driver, scaleParameters,w_ids[i]))
        loader_results.append(r)
    ## FOR
    
    pool.close()
    logging.info("Waiting for %d loaders to finish" % config['clients'])
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
def startExecution(driverClass, scaleParameters, config):
    logging.info("Creating client pool with %d processes" % config['clients'])
    pool = multiprocessing.Pool(config['clients'])
    
    worker_results = [ ]
    for i in range(config['clients']):
        r = pool.apply_async(executorFunc, (driverClass, scaleParameters, config))
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
def executorFunc(driverClass, scaleParameters, config):
    driver = driverClass(config['ddl'])
    assert driver != None
    logging.info("Starting client execution: %s" % driver)

    e = executor.Executor(driver, scaleParameters, stop_on_error=config['stop_on_error'])
    driver.executeStart()
    results = e.execute(config['duration'])
    driver.executeFinish()
    
    return results
## DEF


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--warehouses', default=1, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--clients', default=1, type=int, metavar='N',
                         help='The number of blocking clients to fork')
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')


    args = vars(aparser.parse_args())
    config = dict(map(lambda x: (x, config.POSTGRESQL_CONFIG[x][1]), config.POSTGRESQL_CONFIG.keys()))
    for key,val in args.items():    config[key]=val

    ## Create a handle to the target client driver
    driver = PostgresqlDriver(ddl)

    driver.InitDBHandler(config)

    ## Create ScaleParameters
    scaleParameters = scaleparameters.makeWithScaleFactor(config['warehouses'], config['scalefactor'])
    nurand = rand.setNURand(rand.makeForLoad())
    
    
    ## DATA LOADER!!!
    load_time = None
    if config['reset']:
        driver.reset()

        logging.info("Loading TPC-C benchmark data using %s" % (driver))
        load_start = time.time()
        if config['clients'] == 1:
            l = loader.Loader(driver, scaleParameters, range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1), True)
            driver.loadStart()
            l.execute()
            driver.loadFinish()
        else:
            startLoading(driver, scaleParameters, config)
        load_time = time.time() - load_start
    

    # WORKLOAD DRIVER!!!
    if not config['no_execute']:
        if config['clients'] == 1:
            e = executor.Executor(driver, scaleParameters, stop_on_error=config['stop_on_error'])
            driver.executeStart()
            results = e.execute(config['duration'])
            driver.executeFinish()
        else:
            results = startExecution(driver, scaleParameters,config)
        assert results
        print(results.show(load_time))
    # IF
    
## MAIN