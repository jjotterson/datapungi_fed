import pandas as pd
import requests
import sys
#from datapungi_fed import generalSettings 
from datapungi_fed import generalSettings 
from datapungi_fed import drivers
#import drivers

class data():
    '''
       the purpose of this class is to provide an environment where the shared data needed to establish a connection is loaded
       and to be a one stop shop of listing all available drivers.  
       :param connectionParameters: a dictionary with at least 'key', and 'url'
         {'key': 'your key', 'description': 'FED data', 'url': ''} 
       :param userSettings: settings saved in the packge pointing to a yaml/json or env containing the connection parameters 
    '''
    def __init__(self,connectionParameters = {}, userSettings = {}):
        self.__connectInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings ) 
        self._metadata = self.__connectInfo.packageMetadata
        self._help     = self.__connectInfo.datasourceOverview
        #load drivers:
        loadInfo = {'baseRequest' : self.__connectInfo.baseRequest, 'connectionParameters' : self.__connectInfo.connectionParameters}
        self.datasetlist  = drivers.datasetlist(**loadInfo)
        self.categories   = drivers.categories(**loadInfo)
        self.releases     = drivers.releases(**loadInfo)
        self.series       = drivers.series(**loadInfo)
        self.sources      = drivers.sources(**loadInfo)
        self.tags         = drivers.tags(**loadInfo)
             
    def __call__(self,*args,**kwargs):
        return(self.series(*args,**kwargs))

    def __str__(self):
        return('\nList of drivers and their shortcuts')

    def _clipcode(self):
        try:
            self._lastCalledDriver.clipcode()
        except:
            print('Get data using a driver first, eg: ')
            #eg: data.NIPA("T10101", verbose = True)
    
    def _docDriver(self,driverName,printHelp=True):
        '''
          Given the delegated method name, get the __doc__ of its class.  
          eg: 
          returns the __doc__ of the main method inside the driver.
        '''
        #eg: _docDriver('NIPA') 
        #parentName = list(self.DELEGATED_METHODS.keys())[list(self.DELEGATED_METHODS.values()).index([driverName])]
        #outhelp = getattr(getattr(self,parentName ),driverName).__doc__
        #if printHelp:
        #    print(outhelp)
        #return(outhelp)
        return('')

if __name__ == '__main__':            
    d = data()
    print(d)
    print(d.datasetlist())   
    print(d.categories(125))   
    print(d.releases())   
    print(d.series('GDP'))
    print(d('GNP'))
    print(d.sources('1'))   
    print(d.tags(tag_names='monetary+aggregates;weekly'))   