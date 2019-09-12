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
        dataPath = utils.getResourcePath('/config/datasetlist.yaml')
        with open(dataPath, 'r') as yf:
            datasetlist = yaml.safe_load(yf)
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
       
    def _dbParameters(self,dbGroupName = ''):
        '''
          The parameters of each database in the group (will be assigned empty by default)
        '''  
        dataPath = utils.getResourcePath('/config/datasetlist.yaml')
        with open(dataPath, 'r') as yf:
            datasetlist = yaml.safe_load(yf)

        
        #get the entry of the group:
        datasets = list(filter( lambda x: x['group'] == dbGroupName , datasetlist))[0]['datasets']
        removeCases = lambda array: list(filter( lambda x: x not in ['api_key','file_type']  , array ))
        dbParams = { entry['short name'] : { 'urlSuffix' : entry['database'] , 'params': removeCases(entry['parameters']) } for entry in datasets }
        
        return(dbParams)


class releases(driverCore):
    def releases(self,          
                 file_type='json',
                 params={},
                 verbose=False,
                 warningsOn=True
                 ):
        '''
        Get all releases of economic data.
        Args:
         params
         file_type='json',
         params={},
         verbose=False,
         warningsOn=True
        '''
        # get requests' query inputs
        localVars = locals()  # to get the entries passed in method - the query params
        # variables that aren't query params.
        nonQueryArgs = ['self', 'api', 'params', 'verbose', 'warningsOn']
        warningsList = ['countPassLimit']  # warn on this events.
        # TODO: if api = '' then prefixUrl = 'series'
        output = self._queryApiCleanOutput(
            '', api, localVars, self.releases, params, nonQueryArgs, warningsList, warningsOn, verbose)
        return(output)

    def _cleanOutput(self, api, query, retrivedData):
        if api == "observations":
            dataKey = 'observations'
        elif api == 'series':
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

    def _dbParameters(self):
        '''
          The parameters of each database in the group (will be assigned empty by default)
        '''    
        dbParams = {
          'releases'		     : {'urlSuffix' : 'releases'		    ,'params' : ['realtime_start', 'realtime_end', 'limit', 'offset', 'order_by', 'sort_order']},
          'releases/dates'       : {'urlSuffix' : 'releases/dates'      ,'params' : ['realtime_start', 'realtime_end', 'limit', 'offset', 'order_by', 'sort_order', 'include_release_dates_with_no_data']},
          'release'		         : {'urlSuffix' : 'release'		        ,'params' : ['release_id', 'realtime_start', 'realtime_end']},
          'release/dates'	     : {'urlSuffix' : 'release/dates'	    ,'params' : ['release_id', 'realtime_start', 'realtime_end', 'limit', 'offset', 'sort_order', 'include_release_dates_with_no_data']},
          'release/series'       : {'urlSuffix' : 'release/series'      ,'params' : ['release_id', 'realtime_start', 'realtime_end', 'limit', 'offset', 'order_by', 'sort_order', 'filter_variable', 'filter_value', 'tag_names', 'exclude_tag_names']},
          'release/sources'      : {'urlSuffix' : 'release/sources'     ,'params' : ['release_id', 'realtime_start', 'realtime_end']},
          'release/tags'	     : {'urlSuffix' : 'release/tags'	    ,'params' : ['release_id', 'realtime_start', 'realtime_end', 'tag_names', 'tag_group_id', 'search_text', 'limit', 'offset', 'order_by', 'sort_order']},
          'release/related_tags' : {'urlSuffix' : 'release/related_tags','params' : ['release_id', 'realtime_start', 'realtime_end', 'tag_names', 'exclude_tag_names', 'tag_group_id', 'search_text', 'limit', 'offset', 'order_by', 'sort_order']},
          'release/tables'		 : {'urlSuffix' : 'release/tables'		,'params' : ['release_id', 'element_id', 'include_observation_values', 'observation_date']},
        }
        return(dbParams)

class getSeries(driverCore):
    def series(self,
               series_id='', 
               frequency='',
               units='',
               vintage_dates='',
               api='observations',
               series_search_text='',search_text='', search_type='',
               tag_group_id='',tag_search_text='',
               observation_start='',observation_end='',
               aggregation_method='',
               filter_variable='', filter_value='',
               tag_names='',  exclude_tag_names='',
               output_type='', order_by='',sort_order='',
               realtime_start='',realtime_end='',
               limit='',offset='',
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
            api  (str):               | Args of the API 
            file_type (str):  choose between 'json' (default) or 'xml'
            params (dict):  override all other options with the entries of this dictionary.  (default {})
            verbose (bool): returns data in a pandas dataframe format (default) or dataframe and all data if True.
            warningsOn (bool): print warning if dataset size is larger than the download limit
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        # get requests' query inputs
        localVars = locals()  # to get the entries passed in method - the query params
        # variables that aren't query params.
        nonQueryArgs = ['self', 'api', 'params', 'verbose', 'warningsOn']
        warningsList = ['countPassLimit']  # warn on this events.
        # TODO: if api = '' then prefixUrl = 'series'
        output = self._queryApiCleanOutput(
            'series/', api, localVars, self.series, params, nonQueryArgs, warningsList, warningsOn, verbose)
        return(output)

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

    def _dbParameters(self):
        '''
          The parameters of each database in the group (will be assigned empty by default)
        '''    
        dbParams = {
              'observations'          :	{ 'urlSuffix': 'observations', 'params': [ 'series_id', 'realtime_start', 'realtime_end', 'limit', 'offset', 'sort_order', 'observation_start', 'observation_end', 'units', 'frequency', 'aggregation_method', 'output_type', 'vintage_dates']},
              '' 	                  :	{ 'urlSuffix': '', 'params': [ 'series_id', 'realtime_start', 'realtime_end']},
              'categories'	          :	{ 'urlSuffix': 'categories', 'params': [ 'series_id', 'realtime_start', 'realtime_end']},
              'release'		          :	{ 'urlSuffix': 'release', 'params': [ 'series_id', 'realtime_start', 'realtime_end']},
              'search'		          :	{ 'urlSuffix': 'search', 'params': [ 'series_id', 'realtime_start', 'realtime_end']},
              'search/tags'	          :	{ 'urlSuffix': 'search/tags', 'params': [ 'series_search_text', 'realtime_start', 'realtime_end', 'tag_names', 'tag_group_id', 'tag_search_text', 'limit', 'offset', 'order_by', 'sort_order']},
              'search/related_tags'	  :	{ 'urlSuffix': 'search/related_tags', 'params': [ 'series_search_text', 'realtime_start', 'realtime_end', 'tag_names', 'exclude_tag_names', 'tag_group_id', 'tag_search_text', 'limit', 'offset', 'order_by', 'sort_order']},
              'tags'		          :	{ 'urlSuffix': 'tags', 'params': [ 'series_id', 'realtime_start', 'realtime_end', 'order_by','sort_order']},
              'updates'		          :	{ 'urlSuffix': 'updates', 'params': [ 'realtime_start', 'realtime_end', 'limit', 'offset', 'filter_value', 'start_time', 'end_time']},
              'vintagedates'          :	{ 'urlSuffix': 'vintagedates', 'params': [ 'series_id', 'realtime_start', 'realtime_end', 'limit', 'offset', 'sort_order']},
        }    
        return(dbParams)

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
    v = categories()
    print(v(125))
    print(v['category'](125))
    print(v['category'].options)
    

    # releases
    #d = getReleases()
    #v = d.releases()
    #print(v)

    # sources
    #d = getSources()
    #v = d.sources()
    #v = d.sources('source', '1')
    #v = d.sources('source/releases','1')

    # series
    #d = getSeries()
    #v = d.series('GDP')
    #print(v, v.meta)
    # print(v,v.meta)

