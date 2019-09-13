class F(object):
    
    def __init__(self,a,b):
        self.a = 1
        self.b = 2
        self.ff = {}
        self.ff['aa'] = self.f1
        self.ff['bb'] = self.f2
    def f1(self,x):
        '''
          test
        '''
        return(x)
    def f2(self,x,y):
        return(y+x+self.a)
    def __getitem__(self,key):
        return( self.ff[key])
    def __call__(self,*args,**kwargs):
        #kwargs = {key:locals()[key] for key in inspect.getfullargspec(self.f2).args}
        return()

with open(os.getcwd()+'/datapungi_fed/config/datasetlist.yaml') as yf:
    d = yaml.safe_load(yf)



if __name__ == '__main__':
    f = F(1,2)
    print(f['bb'](1))
    print(f(1,2)        )



[api_key,file_type,search_text,search_type,realtime_start,realtime_end,limitoffset,order_by,sort_order,filter_variable,filter_value,tag_names,exclude_tag_names]