'''
   Base driver class
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

class driverCore():
    def __init__(self,baseRequest={},connectionParameters={},userSettings={}):
        self._connectionInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
        self._baseRequest    = self._getBaseRequest(baseRequest,connectionParameters,userSettings)  
        self._lastLoad       = {}  #data stored here to assist functions such as clipcode        
    
    def _getBaseRequest(self,baseRequest={},connectionParameters={},userSettings={}):
        '''
          Write a base request.  This is the information that gets used in most requests such as getting the userKey
        '''
        if baseRequest =={}:
           connectInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
           return(connectInfo.baseRequest)
        else:
           return(baseRequest)

    def _getBaseCode(self,codeEntries): 
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

    def _getCode(self,query,userSettings={},pandasCode=""):
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