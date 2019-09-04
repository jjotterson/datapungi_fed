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
class getCategories():
    def __init__(self,baseRequest={},connectionParameters={},userSettings={}):
        self._connectionInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
        self._baseRequest    = _getBaseRequest(baseRequest,connectionParameters,userSettings)  
        self._lastLoad       = {}  #data stored here to assist functions such as clipcode
    
    def categories(self,params = {},verbose=False):
        """
        ** describe API here **  
        Sample run -
          {callMethod}()
        
        Args:
            verbose (bool): returns data in a pandas dataframe format or all available information (default) or all data if True.
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        query = deepcopy(self._baseRequest)
        #update basequery with passed data, eg:
        #query['params'].update({'method':'GETDATASETLIST'})  #update with some further base request code.
        #query['params'].update({'TABLENAME': tableName})
        #query['params'].update({'FREQUENCY':frequency})
        #query['params'].update({'YEAR':year})
        #query['params'].update(payload)
        
        retrivedData = requests.get(**query)
        
        df_output = self._cleanOutput(query,retrivedData)
        
        if verbose == False:
            self._lastLoad = df_output
            return(df_output)
        else:
            code = _getCode(query,self._connectionInfo.userSettings,self._cleanCode)
            output = dict(dataFrame = df_output, request = retrivedData, code = code)  
            self._lastLoad = output
            return(output)  
    
    def _cleanOutput(self,query,retrivedData):
            df_output = pd.DataFrame()
            #sample cleaning code:
            #self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['BEAAPI']['Results']['Dataset'] )"
            #df_output =  pd.DataFrame( retrivedData.json()['BEAAPI']['Results']['Dataset'] )
            return(df_output)
        
    def clipcode(self):
        _clipcode(self)
    
    def _driverMetadata(self):
        self.metadata =     [{
            "displayName":"categories",
            "method"     :"categories",   #Name of driver main function - run with getattr(data,'datasetlist')()
            "params"     :{},
        }]


class getTags():
    def __init__(self,baseRequest={},connectionParameters={},userSettings={}):
        self._connectionInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
        self._baseRequest    = _getBaseRequest(baseRequest,connectionParameters,userSettings)  
        self._lastLoad       = {}  #data stored here to assist functions such as clipcode
    
    def tags(self,params = { 'category_id': '125', 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },verbose=False):
        """
        ** describe API here **  
        Sample run -
          {callMethod}()
        
        Args:
            verbose (bool): returns data in a pandas dataframe format or all available information (default) or all data if True.
        Returns:
            output: either a pandas dataframe or a dictionary (verbose=True) with dataFrame, request, and code              
        """
        query = deepcopy(self._baseRequest)
        #update basequery with passed data, eg:
        #query['params'].update({'method':'GETDATASETLIST'})  #update with some further base request code.
        #query['params'].update({'TABLENAME': tableName})
        #query['params'].update({'FREQUENCY':frequency})
        #query['params'].update({'YEAR':year})
        #query['params'].update(payload)
        
        retrivedData = requests.get(**query)
        
        df_output = self._cleanOutput(query,retrivedData)
        
        if verbose == False:
            self._lastLoad = df_output
            return(df_output)
        else:
            code = _getCode(query,self._connectionInfo.userSettings,self._cleanCode)
            output = dict(dataFrame = df_output, request = retrivedData, code = code)  
            self._lastLoad = output
            return(output)  
    
    def _cleanOutput(self,query,retrivedData):
            df_output = pd.DataFrame()
            #sample cleaning code:
            #self._cleanCode = "df_output =  pd.DataFrame( retrivedData.json()['BEAAPI']['Results']['Dataset'] )"
            #df_output =  pd.DataFrame( retrivedData.json()['BEAAPI']['Results']['Dataset'] )
            return(df_output)
        
    def clipcode(self):
        _clipcode(self)
    
    def _driverMetadata(self):
        self.metadata =     [{
            "displayName":"tags",
            "method"     :"tags",   #Name of driver main function - run with getattr(data,'datasetlist')()
            "params"     :{ 'category_id': '125', 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },
        }]

if __name__ == '__main__':
    print(_getBaseRequest())
