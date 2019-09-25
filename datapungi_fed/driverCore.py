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
from textwrap import dedent
from datapungi_fed import generalSettings        #NOTE: projectName 
#import generalSettings        #NOTE: projectName 
from datapungi_fed import utils                  #NOTE: projectName  
#import utils                  #NOTE: projectName  

class driverCore():
    def __init__(self,dbGroupName='',defaultQueryFactoryEntry='', baseRequest={},connectionParameters={},userSettings={}):
        self._queryFactory   = {}  #specific drivers will populate this.    
        self._queryClass     = queryDB({},baseRequest,connectionParameters,userSettings)
        #specific to fed data:
        if dbGroupName:
            self.dbGroupName = dbGroupName
            self.dbParams = self._dbParameters(self.dbGroupName)
            self._queryClass = queryDB(self.dbParams,baseRequest,connectionParameters,userSettings)
            self.queryFactory = { dbName : self._selectDBQuery(self._query, dbName)  for dbName in self.dbParams.keys() }
            self.defaultQueryFactoryEntry = defaultQueryFactoryEntry  #the entry in query factory that __call__ will use.
    
    def _query(self,*args,**kwargs):
        return( self._queryClass.query(*args,**kwargs) )

    def __getitem__(self,dbName):
        return(self.queryFactory[dbName])
    
    def __call__(self,*args,**kwargs):
        out = self.queryFactory[self.defaultQueryFactoryEntry](*args,**kwargs)
        return(out)
        
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
    

class queryDB():
    '''
      queries a db given its dbName and its dbParameters.
    '''
    def __init__(self,dbParams = {},baseRequest={},connectionParameters={},userSettings={}):
        self._connectionInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
        self._baseRequest    = self.getBaseRequest(baseRequest,connectionParameters,userSettings)  
        self._lastLoad       = {}  #data stored here to assist functions such as clipcode    
        self._getCode        = getCode()  
        self.dbParams        = dbParams
        self._cleanCode      = ""  #TODO: improvable - this is the code snippet producing a pandas df
    
    def query(self,dbName,params={},file_type='json',verbose=False,warningsOn=True):
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
        output = self.queryApiCleanOutput(prefixUrl, dbName, params, warningsList, warningsOn, verbose)
        return(output)
     
    def queryApiCleanOutput(self,urlPrefix,dbName,params,warningsList,warningsOn,verbose):
        '''
            Core steps of querying and cleaning data.  Notice, specific data cleaning should be 
            implemented in the specific driver classes

            Args:
                self - should containg a base request (url)
                urlPrefix (str) - a string to be appended to request url (eg, https:// ...// -> https//...//urlPrefix?)
                
                params (dict) - usually empty, override any query params with the entries of this dictionary
                warningsList (list) - the list of events that can lead to warnings
                warningsOn (bool) - turn on/off driver warnings
                verbose (bool) - detailed output or short output
        '''
        
        #get data 
        query = self.getBaseQuery(urlPrefix,params)
        retrivedData = requests.get(**query)
        
        #clean data
        df_output = self.cleanOutput(dbName,query,retrivedData)
        
        #print warning if there is more data the limit to download
        for entry in warningsList:
            self._warnings(entry,retrivedData,warningsOn) 
        
        #short or detailed output, update _lastLoad attribute:
        output = self.formatOutputupdateLoadedAttrib(query,df_output,retrivedData,verbose)
        
        return(output)
    
    def getBaseQuery(self,urlPrefix,params):
        '''
          Return a dictionary of request arguments.

          Args:
              urlPrefix (str) - string appended to the end of the core url (eg, series -> http:...\series? )
              dbName (str) - the name of the db being queried
              params (dict) - a dictionary with request paramters used to override all other given parameters
          Returns:
              query (dict) - a dictionary with 'url' and 'params' (a string) to be passed to a request
        '''
        query = deepcopy(self._baseRequest)
        
        #update query url
        query['url'] = query['url']+urlPrefix   
        query['params'].update(params)
        query['params'] = '&'.join([str(entry[0]) + "=" + str(entry[1]) for entry in query['params'].items()])
        
        return(query)
    
    def formatOutputupdateLoadedAttrib(self,query,df_output,retrivedData,verbose):
        if verbose == False:
            self._lastLoad = df_output
            return(df_output)
        else:
            code = self._getCode.getCode(query,self._baseRequest,self._connectionInfo.userSettings,self._cleanCode)
            output = dict(dataFrame = df_output, request = retrivedData, code = code)  
            self._lastLoad = output
            return(output)
    
    def cleanOutput(self,dbName,query,retrivedData):
        '''
         This is a placeholder - specific drivers should have their own cleaning method
         this generates self._cleanCode
        '''
        return(retrivedData)
    
    def getBaseRequest(self,baseRequest={},connectionParameters={},userSettings={}):
        '''
          Write a base request.  This is the information that gets used in most requests such as getting the userKey
        '''
        if baseRequest =={}:
           connectInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings )
           return(connectInfo.baseRequest)
        else:
           return(baseRequest)
    
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

class getCode():
    def getCode(self,query,baseRequest,userSettings={},pandasCode=""):
        #general code to all drivers:
        try:
            url        = query['url']
            if not userSettings:  #if userSettings is empty dict 
                    apiKeyPath = generalSettings.getGeneralSettings( ).userSettings['ApiKeysPath']
            else:
                apiKeyPath = userSettings['ApiKeysPath']
        except:
            url        = " incomplete connection information "
            apiKeyPath = " incomplete connection information "
        
        #load code header - get keys
        apiCode = self.getApiCode()
        
        #load request's code
        queryCode = self.getQueryCode(query,baseRequest,pandasCode)
        
        return(apiCode + queryCode)
    
    def getQueryCode(self,query,baseRequest,pandasCode=""):
        queryClean = deepcopy(query)
        queryClean['url'] = 'url'
        queryClean['params']=queryClean['params'].replace(baseRequest['params']['api_key'],'{}')+'.format(key)'  #replace explicit api key by the var "key" poiting to it.
        
        queryCode = '''\
            query = {}
            retrivedData = requests.get(**query)
            
            {} #replace json by xml if this is the request format
        '''
        
        queryCode = dedent(queryCode).format(json.dumps(queryClean),pandasCode)
        queryCode = queryCode.replace('"url": "url"', '"url": url')
        queryCode = queryCode.replace('.format(key)"', '".format(key)')
        queryCode = queryCode.replace('"UserID": "key"', '"UserID": key')  #TODO: need to handle generic case, UserID, api_key...        
        return(queryCode)

    def getApiCode(self): 
        '''
          The base format of a code that can be used to replicate a driver using Requests directly.
        '''
        userSettings = utils.getUserSettings()
        pkgConfig    = utils.getPkgConfig()
        storagePref  = userSettings['ApiKeysPath'].split('.')[-1]
        
        passToCode = {'ApiKeyLabel':userSettings["ApiKeyLabel"], "url":pkgConfig['url'], 'ApiKeysPath':userSettings['ApiKeysPath']}
        
        code = self.apiCodeOptions(storagePref)
        code = code.format(**passToCode)   
        
        return(code)
    
    def apiCodeOptions(self,storagePref):
        ''''
          storagePref: yaml, json, env
        '''
        if storagePref == 'yaml':
            code = '''\
                import requests
                import yaml 
                import pandas as pd
                
                apiKeysFile = '{ApiKeysPath}'
                with open(apiKeysFile, 'r') as stream:
                    apiInfo= yaml.safe_load(stream)
                    url,key = apiInfo['{ApiKeyLabel}']['url'], apiInfo['{ApiKeyLabel}']['key']
            '''
        elif storagePref == 'json':
            code = '''\
                import requests
                import json    
                import pandas as pd
                
                # json file should contain: {"BEA":{"key":"YOUR KEY","url": "{url}" }
                
                apiKeysFile = '{ApiKeysPath}'
                with open(apiKeysFile) as jsonFile:
                   apiInfo = json.load(jsonFile)
                   url,key = apiInfo['{ApiKeyLabel}']['url'], apiInfo['{ApiKeyLabel}']['key']    
            '''
        else: #default to env
            code = '''\
                import requests
                import os 
                import pandas as pd
                
                url = "{url}"
                key = os.getenv("{ApiKeyLabel}") 
            '''
        return(dedent(code))
    
    def clipcode(self):
        '''
           Copy the string to the user's clipboard (windows only)
        '''
        try:
            pyperclip.copy(self._lastLoad['code'])
        except:
            print("Loaded session does not have a code entry.  Re-run with verbose option set to True. eg: v.drivername(...,verbose=True)")



if __name__ == '__main__':
    case = driverCore(dbGroupName = 'Series',defaultQueryFactoryEntry='observations')