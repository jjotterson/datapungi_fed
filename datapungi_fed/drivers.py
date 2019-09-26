'''
   Construct driver connecting to databases that are not part of the driverCore.py 
'''

import pandas as pd
import requests
import json
from copy import deepcopy
import pyperclip
import math
import re
import inspect
import yaml
import itertools
import warnings

from datetime import datetime
from datapungi_fed import generalSettings  # NOTE: projectName
#import generalSettings        #NOTE: projectName
from datapungi_fed import utils  # NOTE: projectName
#import utils                  #NOTE: projectName
from datapungi_fed.driverCore import driverCore
#from driverCore import driverCore

#TODO: decorate _query of series to handle: arrays of symbols, tuples of symbols
#TODO: decorate _query of series to have "start" and "end" and set these to "observations_start..."



class datasetlist(driverCore):
    def _query(self):
        '''
         Returns name of available datasets, a short description and their query parameters.
         Args:
           none
         Output:
           - pandas table with query function name, database name, short description and query parameters.
        '''
        #get all dictionary of all drivers (in config/datasetlist.yaml)
        datasetlist = self._dbParams
        datasetlistExp = [[{**entry, **dataset}
                           for dataset in entry.pop('datasets')] for entry in datasetlist]
        datasetlistFlat = list(itertools.chain.from_iterable(
            datasetlistExp))  # flatten the array of array
        df_output = pd.DataFrame(datasetlistFlat)
        return(df_output)
    def __call__(self):
        return(self._query())
    

if __name__ == '__main__':
    #import datapungi_fed as dpf
    d = categories()
    v = d(125);print(v)
    #v = d['category'](125);print(v)
    #v = d['children'](13);print(v)
    #v = d['related'](32073);print(v)
    #v = d['series'](125);print(v)
    #v = d['tags'](125);print(v)
    #v = d['related_tags'](125,tag_names="services;quarterly");print(v)
    
    #d = releases()
    #v = d();print(1,v)
    #v = d['release/dates'](release_id=53); print(2,v)
    #v = d['release'](release_id=53); print(3,v)
    #v = d['release/dates'](release_id=53); print(4,v)
    #v = d['release/series'](release_id=53); print(5,v)
    #v = d['release/sources'](release_id=53); print(6,v)
    #v = d['release/tags'](release_id=53); print(7,v)
    #v = d['release/related_tags'](release_id='86',tag_names='sa;foreign'); print(8,v)
    #v = d['release/tables'](release_id=53); print(9,v)

    d = series()
    print(d._driverMeta)
    v = d['observations']('GDP',verbose=True)
    print(v)
    #v = d['series']('GDP');print(1,v)
    #v = d['categories']('EXJPUS');print(2,v)
    #v = d['observations']('GNP');print(3,v)
    #v = d['release']('IRA');print(4,v)
    #v = d['search'](search_text='monetary+service+index');print(5,v)
    #v = d['search/tags'](series_search_text='monetary+service+index');print(6,v)
    #v = d['search/related_tags'](series_search_text='mortgage+rate',tag_names='30-year;frb');print(7,v)
    #v = d['tags'](series_id='STLFSI');print(8,v)
    #v = d['updates']();print(9,v)
    #v = d['vintagedates']('GNPCA');print(10,v)

    d = tags()
    v = d('monetary+aggregates;weekly');print(1,v)
    v = d['tags']();print(2,v)
    v = d['related_tags'](tag_names='monetary+aggregates;weekly');print(3,v)
    v = d['tags/series'](tag_names='slovenia;food;oecd');print(4,v)
    
    d = datasetlist()
    v = d(); print(v)

    #d = sources()
    #v = d('1')
    #v = d['source/releases']('1')



    
    # print(_getBaseRequest())
    # dataselist
    #d = getDatasetlist()
    #d = datasetlist()
    #print(d())

    # tags
    #d = tags()
    #print(d(tag_names='monetary+aggregates;weekly'))
    #v = d.tags('related_tags', tag_names='monetary+aggregates;weekly')
    #print(v)
    #v = d.tags()  
    #v = d.tags(api='tags/series',tag_names='slovenia;food;oecd')

    # categories
    #v = categories()
    #print(v(125))
    #print(v['category'](125))
    #print(v['category'].options)
    

    # releases
    #d = releases()
    #v = d.releases()
    #print(d())

    # sources
    #d = sources()
    #v = d.sources()
    #print(d('1'))
    #v = d.sources('source/releases','1')

    # series
    #d = series()
    #print(d('GDP'))
    #print(v, v.meta)
    # print(v,v.meta)

