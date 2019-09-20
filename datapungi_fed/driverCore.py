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
import warnings
import functools
from datapungi_fed import generalSettings        #NOTE: projectName 
#import generalSettings        #NOTE: projectName 
from datapungi_fed import utils                  #NOTE: projectName  
#import utils                  #NOTE: projectName  

class driverCore():
    def __init__(self,dbGroupName='',defaultQueryFactoryEntry='', baseRequest={},connectionParameters={},userSettings={}):
        self._connectionInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
        self._baseRequest    = self._getBaseRequest(baseRequest,connectionParameters,userSettings)  
        self._lastLoad       = {}  #data stored here to assist functions such as clipcode    
        self._queryFactory   = {}  #specific drivers will populate this.    
        #specific to fed data:
        if dbGroupName:
            self.dbGroupName = dbGroupName
            self.dbParams = self._dbParameters(self.dbGroupName)
            self.queryFactory = { dbName : self._selectDBQuery(self._query, dbName)  for dbName in self.dbParams.keys() }
            self.defaultQueryFactoryEntry = defaultQueryFactoryEntry  #the entry in query factory that __call__ will use.
    
    def _queryApiCleanOutput(self,urlPrefix,dbName,params,warningsList,warningsOn,verbose):
        '''
            Core steps of querying and cleaning data.  Notice, specific data cleaning should be 
            implemented in the specific driver classes

            Args:
                self - should containg a base request (url)
                urlPrefix (str) - a string to be appended to request url (eg, https:// ...// -> https//...//urlPrefix?)
                api (str) - the database being queried (this gets added to the urlPrefix)
                localVars - the locals() of the main method - basically contain the values of args of the method 
                method - the function itself (driver) used to get the values of method inputs
                params (dict) - usually empty, override any query params with the entries of this dictionary
                nonQueryArgs (list) - the inputs of the method that are not used in a query (eg, verbose)
                warningsList (list) - the list of events that can lead to warnings
                warningsOn (bool) - turn on/off driver warnings
                verbose (bool) - detailed output or short output
        '''
        
        #get data 
        query = self._getBaseQuery(urlPrefix,dbName,params)
        retrivedData = requests.get(**query)
        
        #clean data
        df_output = self._cleanOutput(dbName,query,retrivedData)
        
        #print warning if there is more data the limit to download
        for entry in warningsList:
            self._warnings(entry,retrivedData,warningsOn) 
        
        #short or detailed output, update _lastLoad attribute:
        output = self._formatOutputupdateLoadedAttrib(query,df_output,retrivedData,verbose)
        
        return(output)
    
    def _query(self,dbName,params={},file_type='json',verbose=False,warningsOn=True):
        '''
          Args:
            params
            file_type              
            verbose             
            warningsOn      
        '''
        # get requests' query inputs
        warningsList = ['countPassLimit']  # warn on this events.
        prefixUrl = self.dbParams[dbName]['urlSuffix']
        output = self._queryApiCleanOutput(prefixUrl, dbName, params, warningsList, warningsOn, verbose)
        return(output)
    
    def __getitem__(self,dbName):
        return(self.queryFactory[dbName])
    
    def __call__(self,*args,**kwargs):
        out = self.queryFactory[self.defaultQueryFactoryEntry](*args,**kwargs)
        return(out)
    
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
    
    def _selectDBQuery(self,queryFun,dbName):
        '''
          Fix a generic query to a query to dbName, creates a lambda that, from
          args/kwargs creates a query of the dbName 
        '''
        fun  = functools.partial(queryFun,dbName)
        lfun = lambda *args,**kwargs: fun(**self._getQueryArgs(dbName,*args,**kwargs))
        #add quick user tips
        lfun.options = self.dbParams[dbName]['params']
        return(lfun)
    
    def _cleanOutput(self,api,query,retrivedData):
        '''
         This is a placeholder - specific drivers should have their own cleaning method
        '''
        return(retrivedData)
    
    def _getBaseQuery(self,urlPrefix,dbName,params):
        '''
          Return a dictionary of request arguments.

          Args:
              urlPrefix (str) - string appended to the end of the core url (eg, series -> http:...\series? )
              api (str) - (Specific to datapungi_fed) the name of the database (eg, categories)
              locals    - local data of othe method - to get the passed method arguments
              method (func) - the actual method being called (not just a name, will use this to gets its arguments. eg, driver's main method)
              params (dict) - a dictionary with request paramters used to override all other given parameters 
              removeMethodArgs (list) - the arguments of the method that are not request parameters (eg, self, params, verbose)
          Returns:
              query (dict) - a dictionary with 'url' and 'params' (a string) to be passed to a request
        '''
        query = deepcopy(self._baseRequest)
        
        #update query url
        query['url'] = query['url']+urlPrefix
          
        #update basequery with passed parameters 
        #allArgs = inspect.getfullargspec(method).args
        #inputParams = { key:localVars[key] for key in allArgs if key not in removeMethodArgs } #args that are query params
        #inputParams = dict(filter( lambda entry: entry[1] != '', inputParams.items() )) #filter params.
        #
        ##override if passing arg "params" is non-empty:
        ## - ensure symbols such as + and ; don't get sent to url symbols FED won't read
        #query['params'].update(inputParams)       
        query['params'].update(params)
        query['params'] = '&'.join([str(entry[0]) + "=" + str(entry[1]) for entry in query['params'].items()])

        return(query)
    
    def _getBaseRequest(self,baseRequest={},connectionParameters={},userSettings={}):
        '''
          Write a base request.  This is the information that gets used in most requests such as getting the userKey
        '''
        if baseRequest =={}:
           connectInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
           return(connectInfo.baseRequest)
        else:
           return(baseRequest)
    
    def _formatOutputupdateLoadedAttrib(self,query,df_output,retrivedData,verbose):
        if verbose == False:
            self._lastLoad = df_output
            return(df_output)
        else:
            code = self._getCode(query,self._connectionInfo.userSettings,self._cleanCode)
            output = dict(dataFrame = df_output, request = retrivedData, code = code)  
            self._lastLoad = output
            return(output)
    
    def _dbParameters(self,dbGroupName = ''):
        '''
          The parameters of each database in the group (will be assigned empty by default)
        '''  
        dataPath = utils.getResourcePath('/config/datasetlist.yaml')
        with open(dataPath, 'r') as yf:
            datasetlist = yaml.safe_load(yf)
        
        if dbGroupName == '':
            return(datasetlist)
        
        #get the entry of the group:
        datasets = list(filter( lambda x: x['group'] == dbGroupName , datasetlist))[0]['datasets']
        removeCases = lambda array: list(filter( lambda x: x not in ['api_key','file_type']  , array ))
        dbParams = { entry['short name'] : { 'urlSuffix' : entry['database'] , 'json key': entry['json key'], 'params': removeCases(entry['parameters']) } for entry in datasets }
        
        return(dbParams)
    
    def _warnings(self,warningName,inputs,warningsOn = True):
        if not warningsOn:
            return
        
        if warningName == 'countPassLimit':
            '''
              warns if number of lines in database exceeds the number that can be downloaded.
              inputs = a request result of a FED API 
            '''
            _count = inputs.json().get('count',1)
            _limit = inputs.json().get('limit',1000)
            if _count > _limit:
              warningText = 'NOTICE: dataset exceeds download limit! Check - count ({}) and limit ({})'.format(_count,_limit)
              warnings.warn(warningText) 
        
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
        
        baseCode = self._getBaseCode([url,apiKeyPath])
        
        #specific code to this driver:
        queryClean = deepcopy(query)
        queryClean['url'] = 'url'
        queryClean['params']=queryClean['params'].replace(self._baseRequest['params']['api_key'],'{}')+'.format(key)'
        
        
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

if __name__ == '__main__':
    case = driverCore(dbGroupName = 'Series',defaultQueryFactoryEntry='observations')