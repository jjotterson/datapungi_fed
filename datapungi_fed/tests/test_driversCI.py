import datapungi_fed as dp
import time
import pandas as pd
import os


def executeCode(stringIn):
    '''
      auxiliary function for tests: get the requests code as a string and try to execute it.
    '''
    try:
        exec(stringIn+'\n')      #exec('print("hi")') #
        return(dict( codeRun = True, codeOutput = locals()['df_output']   ))   #try to output the dataframe called df_output
    except:
        try:
            exec(stringIn)  #if no dataframe called output, try to see it at least can exec the code
            return(dict(codeRun = True, codeOutput = pd.DataFrame([])))
        except:
            return(dict(codeRun = False, codeOutput = pd.DataFrame([])))

# content of test_sample.py
def test_startDriver(cmdopt):
    global data
    if not cmdopt == "":
        connectionParameters = {"key": cmdopt, "url": ""}
    else:
        connectionParameters = {}
    data = dp.data(connectionParameters)
    assert data
    return(data)

def test_categories():
    driver = data.categories(**{},verbose=True)
    #execCode = executeCode(driver['code']) 
    assert driver['request'].status_code == 200  #test if connection was stablished
    assert not driver['dataFrame'].empty         #cleaned up output is not empty
    #assert execCode['codeRun']                   #try to execute the code.
    #assert execCode['codeOutput'].equals(driver['dataFrame']) #test if the output of the code equals the output of the  

def test_tags():
    driver = data.tags(**{ 'category_id': '125', 'file_type': 'json', 'realtime_start': '', 'realtime_end':   '', 'tag_names' : '', 'exclude_tag_names':'', 'tag_group_id': '', 'search_text': '', 'limit':'', 'offset':'', 'order_by':'', 'sort_order':'' },verbose=True)
    #execCode = executeCode(driver['code']) 
    assert driver['request'].status_code == 200  #test if connection was stablished
    assert not driver['dataFrame'].empty         #cleaned up output is not empty
    #assert execCode['codeRun']                   #try to execute the code.
    #assert execCode['codeOutput'].equals(driver['dataFrame']) #test if the output of the code equals the output of the  


if __name__ == '__main__':
    #test_answer('')
    #test_IIP()
    #test_datasetlist()
    #test_getParameterList()
    #test_getParameterValues()
    #test_NIPA()
    #test_fixedAssets()