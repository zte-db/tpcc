import sys
import os
import string
import datetime
import logging
import re
import argparse
import glob
import time 
from pprint import pprint,pformat

from util import *
from runtime import *
import drivers

EMPTY = 0
CMD_LOAD = 1
CMD_EXECUTE = 2
CMD_STOP = 3
LOAD_COMPLETED = 4
EXECUTE_COMPLETED = 5
 
class Message:
    def __init__(self,header=EMPTY,data=None):
        self.header=header
        self.data=data
