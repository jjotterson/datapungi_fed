'''
   Construct drivers connecting to databases. 
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

from datetime import datetime
#from datapungi_fed import generalSettings  # NOTE: projectName
import generalSettings        #NOTE: projectName
#from datapungi_fed import utils  # NOTE: projectName
import utils                  #NOTE: projectName
#from datapungi_fed.driverCore import driverCore
from driverCore import driverCore

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
        datasetlist = self._dbParameters() 
        datasetlistExp = [[{**entry, **dataset}
                           for dataset in entry.pop('datasets')] for entry in datasetlist]
        datasetlistFlat = list(itertools.chain.from_iterable(
            datasetlistExp))  # flatten the array of array
        df_output = pd.DataFrame(datasetlistFlat)
        return(df_output)
    def __call__(self):
        return(self._query())

class categories(driverCore):
    def __init__(self,*args, **kwargs):
        '''
          Initializes a dictionary of db queries
        '''
        super(categories, self).__init__(**kwargs)
        self.dbGroupName = 'Categories'
        self.dbParams = self._dbParameters(self.dbGroupName)
        self.queryFactory = { dbName : self._selectDBQuery(self._query, dbName)  for dbName in self.dbParams.keys() }
        self.defaultQueryFactoryEntry = 'category'  #the entry in query factory that __call__ will use.
    
    def _cleanOutput(self, dbName, query, retrivedData):
        if dbName == "observations":
            dataKey = 'observations'
        elif dbName == 'series':
            dataKey = 'series'
        else:
            dataKey = 'categories'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(
            dataKey)
        df_output = pd.DataFrame(
            retrivedData.json()[dataKey])  # TODO: deal with xml
        setattr(df_output, 'meta', dict(filter(
            lambda entry: entry[0] != dataKey, retrivedData.json().items())))  # TODO: silence warning
        return(df_output)
    
    def _driverMetadata(self):
        self.metadata = [{
            "displayName": "tags",
            # Name of driver main function - run with getattr(data,'datasetlist')()
            "method": "tags",
            "params": {'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names': '', 'exclude_tag_names': '', 'tag_group_id': '', 'search_text': '', 'limit': '', 'offset': '', 'order_by': '', 'sort_order': ''},
        }]


class releases(driverCore):
    def __init__(self,*args, **kwargs):
        '''
          Initializes a dictionary of db queries
        '''
        super(releases, self).__init__(**kwargs)
        self.dbGroupName = 'Releases'
        self.dbParams = self._dbParameters(self.dbGroupName)
        self.queryFactory = { dbName : self._selectDBQuery(self._query, dbName)  for dbName in self.dbParams.keys() }
        self.defaultQueryFactoryEntry = 'releases'  #the entry in query factory that __call__ will use.
    
    def _cleanOutput(self, dbName, query, retrivedData):
        if dbName == "observations":
            dataKey = 'observations'
        elif dbName == 'series':
            dataKey = 'series'
        else:
            dataKey = 'releases'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(
            dataKey)
        df_output = pd.DataFrame(
            retrivedData.json()[dataKey])  # TODO: deal with xml
        setattr(df_output, 'meta', dict(filter(
            lambda entry: entry[0] != dataKey, retrivedData.json().items())))  # TODO: silence warning
        return(df_output)
    
    def _driverMetadata(self):
        self.metadata = [{
            "displayName": "tags",
            # Name of driver main function - run with getattr(data,'datasetlist')()
            "method": "tags",
            "params": {'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names': '', 'exclude_tag_names': '', 'tag_group_id': '', 'search_text': '', 'limit': '', 'offset': '', 'order_by': '', 'sort_order': ''},
        }]

class series(driverCore):
    def __init__(self,*args, **kwargs):
        '''
          Initializes a dictionary of db queries
        '''
        super(series, self).__init__(**kwargs)
        self.dbGroupName = 'Series'
        self.dbParams = self._dbParameters(self.dbGroupName)
        self.queryFactory = { dbName : self._selectDBQuery(self._query, dbName)  for dbName in self.dbParams.keys() }
        self.defaultQueryFactoryEntry = 'observations'  #the entry in query factory that __call__ will use.

    def _cleanOutput(self, api, query, retrivedData):
        if api == "observations":
            dataKey = 'observations'
        elif api == 'series':
            dataKey = 'series'
        else:
            dataKey = 'seriess'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(
            dataKey)
        df_output = pd.DataFrame(
            retrivedData.json()[dataKey])  # TODO: deal with xml
        setattr(df_output, 'meta', dict(filter(
            lambda entry: entry[0] != dataKey, retrivedData.json().items())))  # TODO: silence warning
        return(df_output)

    def _driverMetadata(self):
        self.metadata = [{
            "displayName": "tags",
            # Name of driver main function - run with getattr(data,'datasetlist')()
            "method": "tags",
            "params": {'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names': '', 'exclude_tag_names': '', 'tag_group_id': '', 'search_text': '', 'limit': '', 'offset': '', 'order_by': '', 'sort_order': ''},
        }]

class getSources(driverCore):
    def sources(self,
                api='sources',
                source_id='', search_text='',
                order_by='', sort_order='',
                limit='',offset='',
                realtime_start='',realtime_end='',
                file_type='json',
                params={},
                verbose=False,
                warningsOn=True
                ):
        """
        ** tags extracts  **  
        Sample run -
          {callMethod}()

        Args:
            api  (str): choose between "sources" (default), "source", or "source/release"
            file_type (str):  choose between 'json' (default) or 'xml'
            params (dict):  override all other options with the entries of this dictionary.  (default {})
            verbose (bool): returns data in a pandas dataframe format (default) or dataframe and all data if True.
            warningsOn (bool): print warning if dataset size is larger than the download limit
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        localVars = locals()  # to get the entries passed in method - the query params
        # variables that aren't query params.
        nonQueryArgs = ['self', 'api', 'params', 'verbose', 'warningsOn']
        warningsList = ['countPassLimit']  # warn on this events.

        output = self._queryApiCleanOutput(
            '', api, localVars, self.sources, params, nonQueryArgs, warningsList, warningsOn, verbose)
        return(output)

    def _cleanOutput(self, api, query, retrivedData):
        if api == "source/releases":
            dataKey = 'releases'
        else:
            dataKey = 'sources'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(
            dataKey)
        df_output = pd.DataFrame(
            retrivedData.json()[dataKey])  # TODO: deal with xml
        setattr(df_output, 'meta', dict(filter(
            lambda entry: entry[0] != dataKey, retrivedData.json().items())))  # TODO: silence warning
        return(df_output)

    def _driverMetadata(self):
        self.metadata = [{
            "displayName": "tags",
            # Name of driver main function - run with getattr(data,'datasetlist')()
            "method": "tags",
            "params": {'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names': '', 'exclude_tag_names': '', 'tag_group_id': '', 'search_text': '', 'limit': '', 'offset': '', 'order_by': '', 'sort_order': ''},
        }]

    def _dbParameters(self):
        '''
          The parameters of each database in the group (will be assigned empty by default)
        '''    
        dbParams = {
            'sources'		    :	{'urlSuffix': 'sources'		 , 'params': [  'realtime_start', 'realtime_end', 'limit', 'offset', 'order_by', 'sort_order']},
            'source'		    :	{'urlSuffix': 'source'		 , 'params': [  'source_id', 'realtime_start', 'realtime_end']},
            'source/releases'   :	{'urlSuffix': 'source/releases', 'params': [  'source_id', 'realtime_start', 'realtime_end', 'limit', 'offset', 'order_by', 'sort_order']},
        }
        return(dbParams)

class getTags(driverCore):
    def tags(self,
             api='tags',
             tag_names='',tag_group_id='',exclude_tag_names='',
             realtime_start='',  realtime_end='',
             search_text='',
             order_by='', sort_order='',
             limit='',  offset='',
             file_type='json',
             params={},
             verbose=False,
             warningsOn=True
             ):
        """
        ** tags extracts  **  
        Sample run -
          {callMethod}()

        Args:
            api  (str): choose between "tags", "related_tags", or "tags/series"
              tags	        : [realtime_start, realtime_end, tag_names, tag_group_id, search_text, limit, offset, order_by, sort_order]
              related_tags	: [realtime_start, realtime_end, tag_names, exclude_tag_names, tag_group_id, search_text, limit, offset, order_by, sort_order]
              tags/series	: [tag_names, exclude_tag_names, realtime_start, realtime_end, limit, offset, order_by, sort_order]
            file_type (str):  choose between 'json' (default) or 'xml'
            params (dict):  override all other options with the entries of this dictionary.  (default {})
            verbose (bool): returns data in a pandas dataframe format (default) or dataframe and all data if True.
            warningsOn (bool): print warning if dataset size is larger than the download limit
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        localVars = locals()  # to get the entries passed in method - the query params
        # variables that aren't query params.
        nonQueryArgs = ['self', 'api', 'params', 'verbose', 'warningsOn']
        warningsList = ['countPassLimit']  # warn on this events.

        output = self._queryApiCleanOutput(
            '', api, localVars, self.tags, params, nonQueryArgs, warningsList, warningsOn, verbose)
        return(output)

    def _cleanOutput(self, api, query, retrivedData):
        if api == "tags/series":
            dataKey = 'seriess'
        else:
            dataKey = 'tags'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(
            dataKey)
        df_output = pd.DataFrame(
            retrivedData.json()[dataKey])  # TODO: deal with xml
        setattr(df_output, 'meta', dict(filter(
            lambda entry: entry[0] != dataKey, retrivedData.json().items())))  # TODO: silence warning
        return(df_output)

    def _driverMetadata(self):
        self.metadata = [{
            "displayName": "tags",
            # Name of driver main function - run with getattr(data,'datasetlist')()
            "method": "tags",
            "params": {'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names': '', 'exclude_tag_names': '', 'tag_group_id': '', 'search_text': '', 'limit': '', 'offset': '', 'order_by': '', 'sort_order': ''},
        }]

    def _dbParameters(self):
        '''
          The parameters of each database in the group (will be assigned empty by default)
        '''    
        dbParams = {
            'tags'	        : {'urlSuffix': 'tags',         'params' : ['realtime_start', 'realtime_end', 'tag_names', 'tag_group_id', 'search_text', 'limit', 'offset', 'order_by', 'sort_order']},
            'related_tags'	: {'urlSuffix': 'related_tags', 'params' : ['realtime_start', 'realtime_end', 'tag_names', 'exclude_tag_names', 'tag_group_id', 'search_text', 'limit', 'offset', 'order_by', 'sort_order']},
            'tags/series'	: {'urlSuffix': 'tags/series',  'params' : ['tag_names', 'exclude_tag_names', 'realtime_start', 'realtime_end', 'limit', 'offset', 'order_by', 'sort_order']},
        }
        return(dbParams)

if __name__ == '__main__':
    # print(_getBaseRequest())
    # dataselist
    #d = getDatasetlist()
    d = datasetlist()
    print(d())

    # tags
    #d = getTags()
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
    #d = getSources()
    #v = d.sources()
    #v = d.sources('source', '1')
    #v = d.sources('source/releases','1')

    # series
    d = series()
    print(d('GDP'))
    #print(v, v.meta)
    # print(v,v.meta)

