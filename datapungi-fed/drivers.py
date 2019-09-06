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
from datetime import datetime
#from datapungi-fed import generalSettings        #NOTE: projectName 
import generalSettings        #NOTE: projectName 
#from datapungi-fed import utils                  #NOTE: projectName  
import utils                  #NOTE: projectName  


# (1) Auxiliary functions ######################################################
def _getBaseRequest(baseRequest={},connectionParameters={},userSettings={}):
    '''
      Write a base request.  This is the information that gets used in most requests such as getting the userKey
    '''
    if baseRequest =={}:
       connectInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
       return(connectInfo.baseRequest)
    else:
       return(baseRequest)

def _getBaseCode(codeEntries): 
    '''
      The base format of a code that can be used to replicate a driver using Requests directly.
    '''
    userSettings = utils.getUserSettings()
    pkgConfig    = utils.getPkgConfig()
    storagePref  = userSettings['ApiKeysPath'].split('.')[-1]
    
    passToCode = {'ApiKeyLabel':userSettings["ApiKeyLabel"], "url":pkgConfig['url'], 'ApiKeysPath':userSettings['ApiKeysPath']}
    if storagePref == 'json':
        code = '''
import requests
import json    
import pandas as pd

# json file should contain: {"BEA":{"key":"YOUR KEY","url": "{url}" }

apiKeysFile = '{ApiKeysPath}'
with open(apiKeysFile) as jsonFile:
   apiInfo = json.load(jsonFile)
   url,key = apiInfo['{ApiKeyLabel}']['url'], apiInfo['{ApiKeyLabel}']['key']    
     '''.format(**passToCode)
    
    if storagePref == 'env':
        code = '''
import requests
import os 
import pandas as pd

url = "{url}"
key = os.getenv("{ApiKeyLabel}") 
     '''.format(**passToCode)
    
    if storagePref == 'yaml':
        code = '''
import requests
import yaml 
import pandas as pd

apiKeysFile = '{ApiKeysPath}'
with open(apiKeysFile, 'r') as stream:
    apiInfo= yaml.safe_load(stream)
    url,key = apiInfo['{ApiKeyLabel}']['url'], apiInfo['{ApiKeyLabel}']['key']
     '''
    
    return(code)

def _getCode(query,userSettings={},pandasCode=""):
    #general code to all drivers:
    try:
        url        = query['url']
        if not userSettings:  #if userSettings is empty dict 
                apiKeyPath = generalSettings.getGeneralSettings( ).userSettings['ApiKeysPath']
        else:
            apiKeyPath = userSettings['ApiKeysPath']
    except:
        url         = " incomplete connection information "
        apiKeyPath = " incomplete connection information "
    
    baseCode = _getBaseCode([url,apiKeyPath])
    
    #specific code to this driver:
    queryClean = deepcopy(query)
    queryClean['url'] = 'url'
    queryClean['params']['UserID'] = 'key'
    
    
    queryCode = '''
query = {}
retrivedData = requests.get(**query)

{} #replace json by xml if this is the request format
    '''.format(json.dumps(queryClean),pandasCode)
    
    queryCode = queryCode.replace('"url": "url"', '"url": url')
    queryCode = queryCode.replace('"UserID": "key"', '"UserID": key')
    
    return(baseCode + queryCode)

def _clipcode(self):
    '''
       Copy the string to the user's clipboard (windows only)
    '''
    try:
        pyperclip.copy(self._lastLoad['code'])
    except:
        print("Loaded session does not have a code entry.  Re-run with verbose option set to True. eg: v.drivername(...,verbose=True)")

# (2) Drivers ###################################################################
class getSources():
    def __init__(self,baseRequest={},connectionParameters={},userSettings={}):
        self._connectionInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
        self._baseRequest    = _getBaseRequest(baseRequest,connectionParameters,userSettings)  
        self._lastLoad       = {}  #data stored here to assist functions such as clipcode
    
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
        
    def clipcode(self):
        _clipcode(self)
    
    def _driverMetadata(self):
        self.metadata =     [{
            "displayName":"tags",
            "method"     :"tags",   #Name of driver main function - run with getattr(data,'datasetlist')()
            "params"     :{ 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },
        }]


class getTags():
    def __init__(self,baseRequest={},connectionParameters={},userSettings={}):
        self._connectionInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
        self._baseRequest    = _getBaseRequest(baseRequest,connectionParameters,userSettings)  
        self._lastLoad       = {}  #data stored here to assist functions such as clipcode
    
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
        
    def clipcode(self):
        _clipcode(self)
    
    def _driverMetadata(self):
        self.metadata =     [{
            "displayName":"tags",
            "method"     :"tags",   #Name of driver main function - run with getattr(data,'datasetlist')()
            "params"     :{ 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },
        }]

if __name__ == '__main__':
    #print(_getBaseRequest())
    
    #tags
    #d = getTags()
    #v = d.tags('related_tags',tag_names='monetary+aggregates;weekly')
    #v = d.tags()
    #v = d.tags(api='tags/series',tag_names='slovenia;food;oecd')
    
    #sources
    d = getSources()
    #v = d.sources()
    #v = d.sources('source','1')
    v = d.sources('source/releases','1')

    print(v,v.meta)