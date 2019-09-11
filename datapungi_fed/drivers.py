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
from datapungi_fed import generalSettings  # NOTE: projectName
# import generalSettings        #NOTE: projectName
from datapungi_fed import utils  # NOTE: projectName
# import utils                  #NOTE: projectName
from datapungi_fed.driverCore import driverCore


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
        self.dbParams = self._dbParameters()
        self.queryFactory = { dbName : lambda *args,**kwargs: self._query(dbName=dbName,**self._getQueryArgs(dbName,*args,**kwargs)) for dbName in self.dbParams.keys() }
        
    def _query(self,dbName,params={},file_type='json',verbose=False,warningsOn=True):
        '''
          Args:
            params
            file_type              
            verbose             
            warningsOn      
        '''
        # get requests' query inputs
        localVars = locals()  # to get the entries passed in method - the query params
        # variables that aren't query params.
        nonQueryArgs = ['self', 'api', 'params', 'verbose', 'warningsOn']
        warningsList = ['countPassLimit']  # warn on this events.
        prefixUrl = self.dbParams[dbName]['urlSuffix']
        output = self._queryApiCleanOutput(prefixUrl, dbName, params, nonQueryArgs, warningsList, warningsOn, verbose)
        return(output)
    
    def _getQueryArgs(self,dbName,*args,**kwargs):
        '''
          Map args and kwargs to driver args
        '''
        #paramaters to be passed to a requests query:
        paramArray = self.dbParams[dbName]['params']
        params = dict(zip(paramArray,args))
        paramsAdd = {key:val for key, val in kwargs.items() if key in paramArray}
        params.update(paramsAdd)
        #non query options (eg, verbose)
        otherArgs = {key:val for key, val in kwargs.items() if not key in paramArray}
        return({**{'params':params},**otherArgs})
    
    def _cleanOutput(self, api, query, retrivedData):
        if api == "observations":
            dataKey = 'observations'
        elif api == 'series':
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
    
    def __call__(self,*args,**kwargs):
        allArgs = inspect.getfullargspec(self._query).args
        out = self.queryFactory[''](125)
        return(self._query(dbName = '', *args,**kwargs))
    
    def _dbParameters(self):
        '''
          The parameters of each database in the group (will be assigned empty by default)
        '''    
        dbParams = {
            ''            : {'urlSuffix': 'category' ,              'params': ['category_id']},
            'children'    : {'urlSuffix': 'category/children'     , 'params': ['category_id', 'realtime_start', 'realtime_end']},
            'related'     : {'urlSuffix': 'category/related'      , 'params': ['category_id', 'realtime_start', 'realtime_end']},
            'series'      : {'urlSuffix': 'category/series'       , 'params': ['category_id', 'realtime_start', 'realtime_end', 'limit', 'offset', 'order_by','sort_order', 'filter_variable', 'filter_value', 'tag_names', 'exclude_tag_names']},
            'tags'        : {'urlSuffix': 'category/tags'         , 'params': ['category_id', 'realtime_start', 'realtime_end', 'tag_names', 'tag_group_id', 'search_text', 'limit', 'offset', 'order_by', 'sort_order']},
            'related_tags': {'urlSuffix': 'category/related_tags' , 'params': ['category_id', 'realtime_start', 'realtime_end', 'tag_names', 'exclude_tag_names', 'tag_group_id', 'search_text', 'limit', 'offset', 'order_by']},
        }
        return(dbParams)


class getReleases(driverCore):
    def releases(self,
                 api='releases',
                 include_observation_values='', search_text='',
                 tag_names='', exclude_tag_names='', tag_group_id='',
                 observation_date='', sort_order='', filter_value='',
                 include_release_dates_with_no_data='',
                 release_id='', element_id='',
                 realtime_start='', realtime_end='', order_by='', filter_variable='',
                 offset='', limit='',
                 file_type='json',
                 params={},
                 verbose=False,
                 warningsOn=True
                 ):
        '''
        Get all releases of economic data.

        api (str):              | Possible Args  
          releases		        : [realtime_start, realtime_end, limit, offset, order_by, sort_order]
          releases/dates        : [realtime_start, realtime_end, limit, offset, order_by, sort_order, include_release_dates_with_no_data]
          release		        : [release_id, realtime_start, realtime_end]
          release/dates	        : [release_id, realtime_start, realtime_end, limit, offset, sort_order, include_release_dates_with_no_data]
          release/series        : [release_id, realtime_start, realtime_end, limit, offset, order_by, sort_order, filter_variable, filter_value, tag_names, exclude_tag_names]
          release/sources       : [release_id, realtime_start, realtime_end]
          release/tags	        : [release_id, realtime_start, realtime_end, tag_names, tag_group_id, search_text, limit, offset, order_by, sort_order]
          release/related_tags	: [release_id, realtime_start, realtime_end, tag_names, exclude_tag_names, tag_group_id, search_text, limit, offset, order_by, sort_order]
          release/tables		: [release_id, element_id, include_observation_values, observation_date]
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
              observations (default)  :	[ series_id, realtime_start, realtime_end, limit, offset, sort_order, observation_start, observation_end, 
                                          units, frequency, aggregation_method, output_type, vintage_dates]
              '' 	                  :	[ series_id, realtime_start, realtime_end]
              categories	          :	[ series_id, realtime_start, realtime_end]
              release		          :	[ series_id, realtime_start, realtime_end]
              search		          :	[ series_id, realtime_start, realtime_end]
              search/tags	          :	[ series_search_text, realtime_start, realtime_end, tag_names, tag_group_id, tag_search_text, limit, 
                                          offset, order_by, sort_order]
              search/related_tags	  :	[ series_search_text, realtime_start, realtime_end, tag_names, exclude_tag_names, tag_group_id, 
                                          tag_search_text, limit, offset, order_by, sort_order]
              tags		              :	[ series_id, realtime_start, realtime_end, order_by, sort_order]
              updates		          :	[ realtime_start, realtime_end, limit, offset, filter_value, start_time, end_time]
              vintagedates            :	[ series_id, realtime_start, realtime_end, limit, offset, sort_order]
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
                 sources		  :	[  realtime_start, realtime_end, limit, offset, order_by, sort_order]
                 source		      :	[  source_id, realtime_start, realtime_end]
                 source/releases  :	[  source_id, realtime_start, realtime_end, limit, offset, order_by, sort_order]
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
    v = categories('125')
    print(v())

    # releases
    #d = getReleases()
    #v = d.releases()
    #print(v)

    # sources
    #d = getSources()
    #v = d.sources()
    #v = d.sources('source', '1')
    print(v)
    #v = d.sources('source/releases','1')

    # series
    #d = getSeries()
    #v = d.series('GDP')
    #print(v, v.meta)
    # print(v,v.meta)

