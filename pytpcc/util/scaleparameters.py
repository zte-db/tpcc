# -*- coding: utf-8 -*-

import constants


def makeWithScaleFactor(warehouses, scaleFactor):
    assert scaleFactor >= 1.0

    items = int(constants.NUM_ITEMS/scaleFactor)
    if items <= 0: items = 1
    districts = int(max(constants.DISTRICTS_PER_WAREHOUSE, 1))
    customers = int(max(constants.CUSTOMERS_PER_DISTRICT/scaleFactor, 1))
    newOrders = int(max(constants.INITIAL_NEW_ORDERS_PER_DISTRICT/scaleFactor, 0))

    return ScaleParameters(items, warehouses, districts, customers, newOrders)
## DEF

class ScaleParameters:
    
    def __init__(self, items, warehouses, districtsPerWarehouse, customersPerDistrict, newOrdersPerDistrict):
        assert 1 <= items and items <= constants.NUM_ITEMS
        self.items = items
        assert warehouses > 0
        self.warehouses = warehouses
        self.starting_warehouse = 1
        assert 1 <= districtsPerWarehouse and districtsPerWarehouse <= constants.DISTRICTS_PER_WAREHOUSE
        self.districtsPerWarehouse = districtsPerWarehouse
        assert 1 <= customersPerDistrict and customersPerDistrict <= constants.CUSTOMERS_PER_DISTRICT
        self.customersPerDistrict = customersPerDistrict
        assert 0 <= newOrdersPerDistrict and newOrdersPerDistrict <= constants.CUSTOMERS_PER_DISTRICT
        assert newOrdersPerDistrict <= constants.INITIAL_NEW_ORDERS_PER_DISTRICT
        self.newOrdersPerDistrict = newOrdersPerDistrict
        self.ending_warehouse = (self.warehouses + self.starting_warehouse - 1)
    ## DEF

    def __str__(self):
        out =  "%d items\n" % self.items
        out += "%d warehouses\n" % self.warehouses
        out += "%d districts/warehouse\n" % self.districtsPerWarehouse
        out += "%d customers/district\n" % self.customersPerDistrict
        out += "%d initial new orders/district" % self.newOrdersPerDistrict
        return out
    ## DEF

## CLASS