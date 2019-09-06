import pandas as pd
import requests
#from datapungi-fed import generalSettings 
import generalSettings 
#from datapungi-fed import drivers
import drivers

#TODO: improve delegation (want name of methods - getDatasetlis - to be _get... or be all in a loadedDrivers class etc.  These shouldn't be 
#      easy for user access)
# only initialize a driver if it's being called

class delegator(object):
    def __init__(self):
        self._lastCalledDriver = ''
        
    def __getattr__(self, called_method):

        self._lastCalledMethod = called_method

        def __raise_standard_exception():
            raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, called_method))
        
        def wrapper(*args, **kwargs):
            delegation_config = getattr(self, 'DELEGATED_METHODS', None)
            if not isinstance(delegation_config, dict):
                __raise_standard_exception()
            
            for delegate_object_str, delegated_methods in delegation_config.items():
                if called_method in delegated_methods:
                    break
            else:
                __raise_standard_error()
            
            delegate_object = getattr(self, delegate_object_str, None)
            
            self._lastCalledDriver = delegate_object #NOTE: could use this to track all loaded data etc.  For now, will only use 

            return(getattr(delegate_object, called_method)(*args, **kwargs))
    
        return(wrapper)


class data(delegator):
    '''
       the purpose of this class is to provide an environment where the shared data needed to establish a connection is loaded
       and to be a one stop shop of listing all available drivers.  
       :param connectionParameters: a dictionary with at least 'key', and 'url'
         {'key': 'your key', 'description': 'FED data', 'url': ''} 
       :param userSettings: settings saved in the packge pointing to a yaml/json or env containing the connection parameters 
    '''
    DELEGATED_METHODS = {
                #'getCategories' :  ['categories'],
                #'getReleases'   :  ['releases'],
                #'getSeries'     :  ['series'],
                'getSources'    :  ['sources'],
                'getTags'       :  ['tags'],
            }
    def __init__(self,connectionParameters = {}, userSettings = {}):
        self.__connectInfo = generalSettings.getGeneralSettings(connectionParameters = connectionParameters, userSettings = userSettings ) 
        self._metadata = self.__connectInfo.packageMetadata
        self._help     = self.__connectInfo.datasourceOverview
        #load drivers:
        loadInfo = {'baseRequest' : self.__connectInfo.baseRequest, 'connectionParameters' : self.__connectInfo.connectionParameters}
        #self.getCategories = drivers.getCategories(**loadInfo)
        #self.getReleases   = drivers.getReleases(**loadInfo)
        #self.getSeries     = drivers.getSeries(**loadInfo)
        self.getSources    = drivers.getSources(**loadInfo)
        self.getTags       = drivers.getTags(**loadInfo)
        #self.getGetParameterList           = drivers.getGetParameterList(**loadInfo)
        #TODO: improve loading the drivers 
    
    def __str__(self):
        print(pd.DataFrame.from_dict(self.DELEGATED_METHODS,orient='index',columns=['Shortcut to Driver']))
        return('\nList of drivers and their shortcuts')

    def _clipcode(self):
        try:
            self._lastCalledDriver.clipcode()
        except:
            print('Get data using a driver first, eg: ')
            #eg: data.NIPA("T10101", verbose = True)
    
    def _docDriver(self,driverName):
        '''
          Given the delegated method name, get the __doc__ of its class.  
          eg: 
          returns the __doc__ of the main method inside the driver.
        '''
        #eg: _docDriver('NIPA') 
        parentName = list(self.DELEGATED_METHODS.keys())[list(self.DELEGATED_METHODS.values()).index([driverName])]
        outhelp = getattr(getattr(self,parentName ),driverName).__doc__
        return(outhelp)
        




if __name__ == '__main__':
    d = data()
    print(d)
    print(d.tags())   