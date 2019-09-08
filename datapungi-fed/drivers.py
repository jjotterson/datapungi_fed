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
#from datapungi-fed import generalSettings        #NOTE: projectName 
import generalSettings        #NOTE: projectName 
#from datapungi-fed import utils                  #NOTE: projectName  
import utils                  #NOTE: projectName  
from driverCore import driverCore


class getDatasetlist(driverCore):
    def datasetlist(self):
        '''
         Returns name of available datasets, a short description and their query parameters.

         Args:

         Output:
           - pandas table with query function name, database name, short description and query parameters.
        '''
        dataPath = utils.getResourcePath('/config/datasetlist.yaml')
        with open(dataPath,'r') as yf:
            datasetlist = yaml.safe_load(yf)
        datasetlistExp  = [[ {**entry, **dataset} for dataset in entry.pop('datasets')] for entry in datasetlist]
        datasetlistFlat = list(itertools.chain.from_iterable(datasetlistExp)) #flatten the array of array
        df_output = pd.DataFrame(datasetlistFlat)
        return(df_output)    


class getCategories():
    def categories(
        api = '',
        offset = '',
        realtime_end = '',
        file_type = '',
        exclude_tag_names = '',
        order_by = '',
        sort_order = '',
        filter_value = '',
        tag_names = '',
        category_id = '',
        limit = '',
        filter_variable = '',
        tag_group_id = '',
        api_key = '',
        realtime_start = '',
        search_text = '',
        params                 = {},
        verbose                = False,
        countExceedsLimit      = True
    ):
        pass


class getReleases():

    def releases(
        api = '',
        include_observation_values = '',
        tag_names = '',
        search_text = '',
        observation_date = '',
        sort_order = '',
        filter_value = '',
        limit = '',
        order_by = '',
        include_release_dates_with_no_data = '',
        realtime_end = '',
        file_type = '',
        release_id = '',
        element_id = '',
        realtime_start = '',
        offset = '',
        exclude_tag_names = '',
        tag_group_id = '',
        filter_variable = '',
        params                 = {},
        verbose                = False,
        countExceedsLimit      = True
    ):
        pass 


class getSeries(driverCore):  
    def series(self,
        series_id              =  '',  
        api                    =  'observations',             
        realtime_start         =  '',     
        realtime_end           =  '',
        file_type              =  'json',   
        limit                  =  '', 
        offset                 =  '',       
        sort_order             =  '',
        observation_start      =  '',            
        observation_end        =  '',
        units                  =  '',     
        frequency              =  '',         
        aggregation_method     =  '',   
        output_type            =  '',          
        vintage_dates          =  '',         
        search_text            =  '',                
        search_type            =  '',          
        order_by               =  '',          
        filter_variable        =  '',             
        filter_value           =  '',            
        tag_names              =  '',           
        exclude_tag_names      =  '',           
        series_search_text     =  '',    #(fred/series/search/tags or related_tags args)
        tag_group_id           =  '',    #(fred/series/search/tags or related_tags args)  
        tag_search_text        =  '',    #(fred/series/search/tags or related_tags args) 
        params                 = {},
        verbose                = False,
        countExceedsLimit      = True
    ):
        """
        ** tags extracts  **  
        Sample run -
          {callMethod}()
        
        Args:
            api  (str): choose between "fred/sources" (default), "fred/source", or "fred/source/release"
                - sources - Get all sources of economic data.
                - source - Get a source of economic data.
                - source/releases - Get the releases for a source.
            source_id (str): default to ''
            realtime_start (str):  default to '' - which leads to current date
            realtime_end (str):  default to '' - which leads to current date
            search_text (str): default to ''
            limit (str): default to '' - which leads to a limit of 1000 - the max 
            offset (str): default to ''
            order_by (str):   default to ''
            sort_order (str): default to ''
            file_type (str):  choose between 'json' (default) or 'xml'
            params (dict):  override all other options with the entries of this dictionary.  (default {})
            verbose (bool): returns data in a pandas dataframe format (default) or dataframe and all data if True.
            countExceedsLimit (bool): print warning if dataset size is larger than the download limit
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        query = deepcopy(self._baseRequest)
        
        #update query url
        query['url'] = query['url']+'series/'+api
          
        #update basequery with passed parameters 
        allArgs = inspect.getfullargspec(self.series).args
        localVars = locals()
        inputParams = { key:localVars[key] for key in allArgs if key not in ['self','api','params','verbose','countExceedsLimit'] } #args that are query params
        inputParams = dict(filter( lambda entry: entry[1] != '', inputParams.items() )) #filter params.
        
        #override if passing arg "params" is non-empty:
        # - ensure symbols such as + and ; don't get sent to url symbols FED won't read
        query['params'].update(inputParams)       
        query['params'].update(params)
        query['params'] = '&'.join([str(entry[0]) + "=" + str(entry[1]) for entry in query['params'].items()])
        
        #get data and clean it
        retrivedData = requests.get(**query)
        df_output = self._cleanOutput(api,query,retrivedData)
        
        #print warning if there is more data the limit to download
        _count = retrivedData.json().get('count',1)
        _limit = retrivedData.json().get('limit',1000)
        if _count > _limit and countExceedsLimit:
            print('NOTICE: dataset exceeds download limit! Check - count ({}) and limit ({})'.format(
                _count,_limit))
        if verbose == False:
            self._lastLoad = df_output
            return(df_output)
        else:
            code = _getCode(query,self._connectionInfo.userSettings,self._cleanCode)
            output = dict(dataFrame = df_output, request = retrivedData, code = code)  
            self._lastLoad = output
            return(output)  
    
    def _cleanOutput(self,api,query,retrivedData):
        if   api == "observations":  
            dataKey = 'observations'
        elif api == 'series':
            dataKey = 'series'
        else:
            dataKey = 'seriess'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(dataKey)
        df_output =  pd.DataFrame( retrivedData.json()[dataKey] ) #TODO: deal with xml
        setattr(df_output,'meta', dict(filter( lambda entry: entry[0] != dataKey, retrivedData.json().items() ))) #TODO: silence warning
        return(df_output)
    
    def _driverMetadata(self):
        self.metadata =     [{
            "displayName":"tags",
            "method"     :"tags",   #Name of driver main function - run with getattr(data,'datasetlist')()
            "params"     :{ 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },
        }]

class getSources(driverCore):    
    def sources(self,
        api = 'sources',
        source_id = '',
        realtime_start = '',
        realtime_end = '',
        search_text = '',
        limit='',
        offset='', 
        order_by='', 
        sort_order='',
        file_type = 'json',
        params = {},
        verbose=False,
        countExceedsLimit = True
    ):
        """
        ** tags extracts  **  
        Sample run -
          {callMethod}()
        
        Args:
            api  (str): choose between "fred/sources" (default), "fred/source", or "fred/source/release"
                - sources - Get all sources of economic data.
                - source - Get a source of economic data.
                - source/releases - Get the releases for a source.
            source_id (str): default to ''
            realtime_start (str):  default to '' - which leads to current date
            realtime_end (str):  default to '' - which leads to current date
            search_text (str): default to ''
            limit (str): default to '' - which leads to a limit of 1000 - the max 
            offset (str): default to ''
            order_by (str):   default to ''
            sort_order (str): default to ''
            file_type (str):  choose between 'json' (default) or 'xml'
            params (dict):  override all other options with the entries of this dictionary.  (default {})
            verbose (bool): returns data in a pandas dataframe format (default) or dataframe and all data if True.
            countExceedsLimit (bool): print warning if dataset size is larger than the download limit
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        query = deepcopy(self._baseRequest)
        
        #update query url
        query['url'] = query['url']+api
          
        #update basequery with passed parameters 
        allArgs = inspect.getfullargspec(self.sources).args
        localVars = locals()
        inputParams = { key:localVars[key] for key in allArgs if key not in ['self','api','params','verbose','countExceedsLimit'] } #args that are query params
        inputParams = dict(filter( lambda entry: entry[1] != '', inputParams.items() )) #filter params.
        
        #override if passing arg "params" is non-empty:
        # - ensure symbols such as + and ; don't get sent to url symbols FED won't read
        query['params'].update(inputParams)       
        query['params'].update(params)
        query['params'] = '&'.join([str(entry[0]) + "=" + str(entry[1]) for entry in query['params'].items()])
        
        #get data and clean it
        retrivedData = requests.get(**query)
        df_output = self._cleanOutput(api,query,retrivedData)
        
        #print warning if there is more data the limit to download
        _count = retrivedData.json().get('count',1)
        _limit = retrivedData.json().get('limit',1000)
        if _count > _limit and countExceedsLimit:
            print('NOTICE: dataset exceeds download limit! Check - count ({}) and limit ({})'.format(
                _count,_limit))
        if verbose == False:
            self._lastLoad = df_output
            return(df_output)
        else:
            code = _getCode(query,self._connectionInfo.userSettings,self._cleanCode)
            output = dict(dataFrame = df_output, request = retrivedData, code = code)  
            self._lastLoad = output
            return(output)  
    
    def _cleanOutput(self,api,query,retrivedData):
        if api == "source/releases":  
            dataKey = 'releases'
        else:
            dataKey = 'sources'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(dataKey)
        df_output =  pd.DataFrame( retrivedData.json()[dataKey] ) #TODO: deal with xml
        setattr(df_output,'meta', dict(filter( lambda entry: entry[0] != dataKey, retrivedData.json().items() ))) #TODO: silence warning
        return(df_output)
    
    def _driverMetadata(self):
        self.metadata =     [{
            "displayName":"tags",
            "method"     :"tags",   #Name of driver main function - run with getattr(data,'datasetlist')()
            "params"     :{ 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },
        }]


class getTags(driverCore):
    def tags(self,
        api = 'tags',
        tag_names = '',
        tag_group_id = '',
        exclude_tag_names = '',
        realtime_start = '',
        realtime_end = '',
        search_text = '',
        limit='',
        offset='', 
        order_by='', 
        sort_order='',
        file_type = 'json',
        params = {},
        verbose=False,
        countExceedsLimit = True
    ):
        """
        ** tags extracts  **  
        Sample run -
          {callMethod}()
        
        Args:
            api  (str): choose between "tags", "related_tags", or "tags/series"
                - fred/tags - Get all tags, search for tags, or get tags by name.
                - fred/related_tags - Get the related tags for one or more tags.
                - fred/tags/series - Get the series matching tags.
            tag_names (str):  default to '' need to pass if api is not "tags" eg: monetary+aggregates;weekly   
            tag_group_id (str): default to ''
            exclude_tag_names (str): default to ''
            realtime_start (str):  default to '' - which leads to current date
            realtime_end (str):  default to '' - which leads to current date
            search_text (str): default to ''
            limit (str): default to '' - which leads to a limit of 1000 - the max 
            offset (str): default to ''
            order_by (str):   default to ''
            sort_order (str): default to ''
            file_type (str):  choose between 'json' (default) or 'xml'
            params (dict):  override all other options with the entries of this dictionary.  (default {})
            verbose (bool): returns data in a pandas dataframe format (default) or dataframe and all data if True.
            countExceedsLimit (bool): print warning if dataset size is larger than the download limit
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        query = deepcopy(self._baseRequest)
        
        #update query url
        query['url'] = query['url']+api
          
        #update basequery with passed parameters 
        allArgs = inspect.getfullargspec(self.tags).args
        localVars = locals()
        inputParams = { key:localVars[key] for key in allArgs if key not in ['self','api','params','verbose','countExceedsLimit'] } #args that are query params
        inputParams = dict(filter( lambda entry: entry[1] != '', inputParams.items() )) #filter params.
        
        #override if passing arg "params" is non-empty:
        # - ensure symbols such as + and ; don't get sent to url symbols FED won't read
        query['params'].update(inputParams)       
        query['params'].update(params)
        query['params'] = '&'.join([str(entry[0]) + "=" + str(entry[1]) for entry in query['params'].items()])
        
        #get data and clean it
        retrivedData = requests.get(**query)
        df_output = self._cleanOutput(api,query,retrivedData)
        
        #print warning if there is more data the limit to download
        if retrivedData.json()['count'] > retrivedData.json()['limit'] and countExceedsLimit:
            print('NOTICE: dataset exceeds download limit! Check - count ({}) and limit ({})'.format(
                retrivedData.json()['count'],retrivedData.json()['limit']))
        if verbose == False:
            self._lastLoad = df_output
            return(df_output)
        else:
            code = _getCode(query,self._connectionInfo.userSettings,self._cleanCode)
            output = dict(dataFrame = df_output, request = retrivedData, code = code)  
            self._lastLoad = output
            return(output)  
    
    def _cleanOutput(self,api,query,retrivedData):
        if api == "tags/series":
            dataKey = 'seriess'
        else:
            dataKey = 'tags'
        self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['{}'] )".format(dataKey)
        df_output =  pd.DataFrame( retrivedData.json()[dataKey] ) #TODO: deal with xml
        setattr(df_output,'meta', dict(filter( lambda entry: entry[0] != dataKey, retrivedData.json().items() ))) #TODO: silence warning
        return(df_output)
    
    def _driverMetadata(self):
        self.metadata =     [{
            "displayName":"tags",
            "method"     :"tags",   #Name of driver main function - run with getattr(data,'datasetlist')()
            "params"     :{ 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },
        }]

if __name__ == '__main__':
    #print(_getBaseRequest())
    
    #tags
    d = getTags()
    v = d.tags('related_tags',tag_names='monetary+aggregates;weekly')
    print(v)
    #v = d.tags()
    #v = d.tags(api='tags/series',tag_names='slovenia;food;oecd')
    
    #sources
    d = getSources()
    v = d.sources()
    v = d.sources('source','1')
    print(v)
    #v = d.sources('source/releases','1')
    
    #series
    d = getSeries()
    v = d.series('GDP')
    print(v,v.meta)
    #print(v,v.meta)

    #dataselist
    d = getDatasetlist()
    print(d.datasetlist())