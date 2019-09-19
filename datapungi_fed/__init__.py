""" Gets data from datapungi_fed ({projectName}) by connecting to its API."""
import pandas
import requests
import sys

from datapungi_fed.api import *
import datapungi_fed.tests as tests

__version__ = '0.1.1'


class topCall(sys.modules[__name__].__class__):
    def __call__(self,*args,**kwargs):
        coreClass = data()
        return(coreClass(*args,**kwargs))

sys.modules[__name__].__class__ = topCall