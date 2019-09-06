<!--
TODO: add explanation of the request part of the vintage.
-->

[![image](https://img.shields.io/pypi/v/datapungi-fed.svg)](https://pypi.org/project/datapungi-fed/) 
[![build Status](https://travis-ci.com/jjotterson/datapungi-fed.svg?branch=master)](https://travis-ci.com/jjotterson/datapungi-fed)
[![downloads](https://img.shields.io/pypi/dm/datapungi-fed.svg)](https://pypi.org/project/datapungi-fed/)
[![image](https://img.shields.io/pypi/pyversions/datapungi-fed.svg)](https://pypi.org/project/datapungi-fed/)

install code: pip install datapungi-fed 

<h1> datapungi-fed  </h1>

  datapungi-fed is a python package that provides a simplified way to extract data from the API of Federal Reserve (FED).  Overall it:
  - 
  -      
  - can read a saved API key (in json/yaml files or environment variables (default)) to avoid having a copy of it on a script.
  - can automatically test: 
      * the connectivity to all BEA datasets, 
      * the quality of the cleaned up data, and 
      * if the provided requests code snippet returns the correct result. 

## Sections
  -  [Short sample runs](#Sample-runs)
  -  [Short sample runs of all drivers](#Sample-run-of-all-drivers)
  -  [Description of a full return](#Full-request-result) 
  -  [Setting up datapungi-fed](#Setting-up-datapungi-fed)
  -  [Testing the package](#Running-Tests) 

## Sample runs

First, [set the package up](#Setting-up-datapungi-fed) (get an API key from BEA, save it somewhere and let datapungi-fed know its location).  After setting datapungi-fed up, you can run the following:

```python
'''
  Short datapungi-fed sample run
'''

import datapungi-fed as dpf

data = dpf.data() #or data = dpf.data("API Key"), see setting up section   

#Basic package description
print(data._help)                 #overall info and basic example
print(data)                       #the list of available databases
print(data._docDriver('tags'))    #documentation of a specific databases

#Query a database, return only pandas table:
data.tags()                                  
data.tags().meta                            #eg, query date, count of entries and its load limit (1000 max)
data.tags('related_tags',tag_names='monetary+aggregates;weekly')                                 #eg, query date, count of entries and its load limit (1000 max)
data.NIPA('T10101',frequency='A',year='X')  
data.NIPA('T71800',frequency='A')           #if a query does not work, try other frequencies


#Query a database, return all information:
full = data.NIPA('T10101',verbose=true)  
full['dataFrame']           #pandas table, as above
full['request']             #full request run, see section below
full['code']                #code snippet of a request that reproduces the query. 

data._clipcode() #copy ccode to clipboard (Windows only).
```
 
### Sample run of all drivers

Notice that all panda tables include a "meta" section listing units, short table description, revision date etc.  For more detailed metadata, use the verbose = True option (see, [Description of a full return](#Full-request-result)).  

```python
import datapungi-fed as dpf

data = dpf.data()

v = data.NIPA('T10101')
v.meta
```

Also, "meta" is not a pandas official attribute; slight changes to the dataframe (say, merging, or multiplying it by a number) will remove meta.


```python

import datapungi-fed as dpf

#start the drivers:
data = dpf.data()

#FRED tags dataset:
data.tags()                                  
data.tags(api='related_tags',tag_names='monetary+aggregates;weekly') 
data.tags('tag/series','slovenia;food;oecd') 
    

#specific driver queries:
data.NIPA('T10101')
data.fixedAssets('FAAt101','X')
data.ITA('BalCurrAcct','Brazil','A','2010')
#NOTE: for IIPeither use All years of All TypeOfInvestment            
data.IIP(TypeOfInvestment='DebtSecAssets',Component='All',Frequency='All',Year='All') 
data.IIP('All','All','All','2010')              
data.GDPbyIndustry('211','1','A','2018')

#RegionalIncome and RegionalOutput were deprecated - use Regional instead.
data.getRegionalIncome.RegionalIncome()
data.getRegionalProduct.RegionalProduct()

data.InputOutput(TableID='56',Year='2010')
data.InputOutput('All','All')                       

#NOTE: Next driver's PDF and query of getParameterValues say Frequency = Q, but actually it's A
data.UnderlyingGDPbyIndustry('ALL','ALL','A','ALL') 
data.IntlServTrade('ALL','ALL','ALL','AllCountries','All')  
```

## Full request result 

When the verbose option is selected, eg:

```python
tab = data.NIPA('T10101',verbose = True)
```

A query returns a dictionary with three entries: dataFrame, request and code.  
  - dataFrame is a cleaned up version of the request result in pandas dataframe format
  - request is the full output of a request query (see the request python package)
  - code is a request code snippet to get the data that can be placed in a script 
  - (and "metadata" in some cases - listing detailed metadata)

The most intricate entry is the request one.  It is an object containing the status of the query:

```python
print(tab['request'])  #200 indicates that the query was successfull 
```

and the output:

```python
tab['request'].json()['BEAAPI']
```

a dictionary.  Its entry

```python
 tab['request'].json()['BEAAPI']['Results']
```

is again a dictionary this time with the following entries:
  
  - Statistic: the name of the table (eg, NIPA)
  - UTCProductionTime: the time when you downloaded the data
  - Dimensions: the dimensions (unit of measurement) of each entry of the dataset
  - Data: the dataset 
  - Notes: A quick description of the dataset with the date it was last revised.  



## Setting up datapungi-fed 

To use the FED API, **the first step** is to get an API key from: 

* 

There are three main options to pass the key to datapungi-fed:

#### (Option 1) Pass the key directly:
```python
import datapungi-fed as dpf

data = dpf.data("API KEY")

data.NIPA('T10101')
```

#### (Option 2) Save the key in either a json or yaml file and let datapungi-fed know its location:

 sample json file : 
```python
    {  
         "FED": {"key": "**PLACE YOUR KEY HERE**", "url": ""},
         (...Other API keys...)
    }
```
sample yaml file:

```yaml
FED: 
    key: PLACE API KEY HERE
    description: FED data
    url: 
api2:
    key:
    description:
    url:
```

Now can either always point to the API location on a run, such as:

```python
import datapungi-fed as dpf   
    
userSettings = {
   'ApiKeysPath':'**C:/MyFolder/myApiKey.yaml**', #or .json
   'ApiKeyLabel':'FED',
   'ResultFormat':'JSON'
}   

data = dpf.data(userSettings = userSettings)  
data.NIPA('T10101')
```

Or, save the path to your BEA API key on the package's user settings (only need to run the utils once, datapungi-fed will remember it in future runs):


```python
import datapungi-fed as dpf

dpf.utils.setUserSettings('C:/Path/myKeys.yaml') #or .json

data = dpf.data()
data.NIPA('T10101')
```

#### (Option 3) Save the key in an environment variable

Finally, you can also save the key as an environment variable (eg, windows shell and in anaconda/conda virtual environment).   

For example, on a command prompt (cmd, powershell etc, or in a virtual environment)

```
> set FED=APIKey 
```

Then start python and run:

```python
import datapungi-fed as dpf

data = dpf.data()
data.NIPA('T10101')
```

Notice: searching for an environment variable named 'FED' is the default option.  If changed to some other option and want to return to the default, run:

```python
import datapungi-fed as dpf

dpf.utils.setUserSettings('env')  
```

If you want to save the url of the API in the environment, call it FED_url. datapungi-fed will use the provided http address instead of the default 

> 

### Changing the API key name
  By default, datapungi-fed searches for an API key called 'FED' (in either json/yaml file or in the environment).  In some cases, it's preferable to call it something else (in conda, use FED_Secret to encript it).  To change the name of the key, run

  ```python
  import datapungi-fed as dpf
  
  dpf.utils.setKeyName('FED_Secret')  #or anyother prefered key name
  ```
  When using environment variables, if saving the API url in the environment as well, call it KeyLabel_url (for example, 'FED_Secret_url'). Else, datapungi-fed will use the default one.
  
## Running Tests

datapungi-fed comes with a family of tests to check its access to the API and the quality of the retrieved data.  They check if:

1. the connection to the API is working,
2. the data cleaning step worked,
3. the code snippet is executing,
4. the code snippet produces the same data as the datapungi-fed query.

Other tests check if the data has being updated of if new data is available.  Most of these tests are run every night on python 3.5, 3.6 and 3.7 (see the code build tag on the top of the document).  However, 
these test runs are not currently checking the code snippet quality to check if its output is the same as the driver's. To run the tests, including the one 
that checks code snippet quality, type:

```python
import datapungi-fed as dpf

dpf.tests.runTests(outputPath = 'C:/Your Path/')
```

This will save an html file in the path specified called datapungi-fed_Tests.html

You can save your test output folder in the user settings as well (need / at the end):

```python
import datapungi-fed as dpf

dpf.utils.setTestFolder('C:/mytestFolder/')
```


## References 

