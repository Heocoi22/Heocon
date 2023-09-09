import os,datetime,time
import pandas as pd
import pymongo
from pymongo import MongoClient
from datetime import timedelta, time as _time

#---------------------------------------------------------------------------------------------------------------------
user = 'VNMonitor'
pwd = 'Monitor'
DB_ip = '172.18.136.250'
DB_port = 27017
#mongoinfo = "mongodb://"+user+":"+pwd+"@"+str(DB_ip)+":"+str(DB_port)+"/"
datapath = '.\\Data\\'
datapath_m =  '.\\Data_m\\'
 #---------------------------------------------------------------------------------------------------------------------
db_start_time = datetime.datetime(2022,10,1,0,0)      #Set time begin(yyyy,mm,dd,hh,mm)
db_end_time = datetime.datetime(2022,10,31,23,59) 
#---------------------------------------------------------------------------------------------------------------------

#Model = 'CGN5-AP2'
#Model = 'CGNV5_PRO_MGC'
#Model = 'CODA-5712'
#Model_list = ['CGN5-AP2','CGNV5_PRO_MGC','CGNV5_UNE','CGN5_AP','CODA-5712','CHITA_HUB5'] 
Model_list = ['CODA-5712','CODA-5310','CODA-5519','CODA-5810','CODA-5719','CODA-5512','CODA-5519-HUB6']
#---------------------------------------------------------------------------------------------------------------------
'''conn = MongoClient(mongoinfo)
db = conn[Model]
collection_AM = db['Auto_T0']
collection_AT = db['Auto_PCBA_AUTOMATION']
collection_AW= db['Auto_W11'] '''
#---------------------------------------------------------------------------------------------------------------------


def get_data_cable(model,collection,station):
    start_time = db_start_time
    i=0
    da = pd.DataFrame(columns = ['Start time' , 'End time', 'Total MAC' , 'MAC PASS','MAC FAIL','Retry FAIL'])
    while  start_time < db_end_time :
        end_time =  start_time.replace(minute=0,second=0,microsecond=0) + timedelta(hours = 12)
        df=pd.DataFrame(list(collection.find({"end_time":{"$gte":start_time,"$lt":end_time}})))
        #print (df.tail())
        if 'Empty DataFrame' not in str(df.head()) :
            if df["result"].count() != 0:
                  des= df["result"].describe()
                  #print(des)                    # des[count:,unique:,top:,freq:]
                  if  des[1] == 1 :
                          if  des[2]=="FAIL": f,p,mac=df.result.value_counts().FAIL,0,des[0]
                          else: f,p,mac=0,df.result.value_counts().PASS,des[0]
                          #print ('*******************************************') 
                  else: 
                      #p = df.result.value_counts().PASS        # df["Result"].value_counts().PASS      Count "PASS" value in pandas ----
                      dp = df[df["result"]=='PASS']
                      p = dp['mac'].nunique()
                      f = df.result.value_counts().FAIL
                      mac = df['mac'].nunique()
                      #print ( df["mac"].describe())  
        else: 
            p=0
            f= 0
            mac = 0
        da.loc[i] = [str(start_time).split('.')[0],str(end_time).split('.')[0],mac,p,(mac-p),f]
        start_time = end_time
        
        i += 1
    #print(da)
    da.to_csv('%s%s_%s_%s %s.csv'%(datapath,model,station,str(db_start_time).split('.')[0].split(' ')[0],str(db_end_time).split('.')[0].split(' ')[0]))
    print ('Export data %s OK'%station)
    
    
def get_data_cable_month(model,i,collection,station):
    #start_time = db_start_time
    date_format_str='%Y-%m-%dT%H:%M:%S.%fZ'
    #i=0
    #da = pd.DataFrame(columns = ['Start time' , 'End time', 'Total MAC' , 'MAC PASS','MAC FAIL','Retry FAIL'])
    #while  start_time < db_end_time :
    #end_time =  start_time.replace(minute=0,second=0,microsecond=0) + timedelta(hours = 12)
    df=pd.DataFrame(list(collection.find({"start_time":{"$gte":db_start_time,"$lt":db_end_time}})))
    #print (df.tail())
    if 'Empty DataFrame' not in str(df.head()) :
        time1 =  df.iloc[0]["start_time"] 
        time2 = df.iloc[-1]["start_time"] 
        start = pd.to_datetime(time1, format=date_format_str)
        end = pd.to_datetime(time2, format=date_format_str)
        if df["result"].count() != 0:
              des= df["result"].describe()
              #print(des)                    # des[count:,unique:,top:,freq:]
              if  des[1] == 1 :
                      if  des[2]=="FAIL": f,p,mac=df.result.value_counts().FAIL,0,des[0]
                      else: f,p,mac=0,df.result.value_counts().PASS,des[0]
                      #print ('*******************************************') 
              else: 
                  #p = df.result.value_counts().PASS        # df["Result"].value_counts().PASS      Count "PASS" value in pandas ----
                  dp = df[df["result"]=='PASS']
                  p = dp['mac'].nunique()
                  f = df.result.value_counts().FAIL
                  mac = df['mac'].nunique()
                  #print ( df["mac"].describe())  
    else: 
        p=0
        f= 0
        mac = 0
    #da.loc[i] = [str(start).split('.')[0],str(end).split('.')[0],mac,p,(mac-p),f]

    da.loc[i] = [model,station,str(start).split('.')[0],str(end).split('.')[0],mac,p,(mac-p),f]
    return da.loc[i]    

    #print(da)
    #da.to_csv('%s%s_%s_%s %s.csv'%(datapath_m,model,station,str(db_start_time).split('.')[0].split(' ')[0],str(db_end_time).split('.')[0].split(' ')[0]))
    #da.to_csv('%s%s_%s.csv'%(datapath_m,model,station))
    #print ('Export data %s %s OK'%(Model,station))

def get_data_wifi(model,collection,station):
    start_time = db_start_time
    i=0
    da = pd.DataFrame(columns = ['Start time' , 'End time', 'Total MAC' , 'MAC PASS','MAC FAIL','Retry FAIL'])
    while  start_time < db_end_time :
        end_time =  start_time.replace(minute=0,second=0,microsecond=0) + timedelta(hours = 12)
        df=pd.DataFrame(list(collection.find({"Time":{"$gte":start_time,"$lt":end_time}})))
        #df.to_csv('%s_%s.csv'%(station,i))
        if 'Empty DataFrame' not in str(df.head()) : 
            if df["Result"].count() != 0:
                  des= df["Result"].describe()                    # des[count:,unique:,top:,freq:]
                  if  des[1] == 1 :
                          if  des[2]=="FAIL": p,f,mac=0,df.Result.value_counts().FAIL,des[0]
                          else: f,p,mac=0,df.Result.value_counts().PASS,des[0] 
                  else: 
                      #p = df.Result.value_counts().PASS        # df["Result"].value_counts().PASS      Count "PASS" value in pandas ----
                      dp = df[df["Result"]=='PASS']
                      p = dp['MAC'].nunique()
                      f = df.Result.value_counts().FAIL
                      mac = df['MAC'].nunique()
                      #print ( df["MAC"].describe())  
        else: 
            p=0
            f= 0
            mac = 0
        da.loc[i] = [str(start_time).split('.')[0],str(end_time).split('.')[0],mac,p,(mac-p),f]
        start_time = end_time
        #print ('---------------------------------%s-----------------------------------------'%i)
        i += 1
    #print(da)
    da.to_csv('%s%s_%s_%s %s.csv'%(datapath,model,station,str(db_start_time).split('.')[0].split(' ')[0],str(db_end_time).split('.')[0].split(' ')[0]))
    print ('Export data %s OK'%station)


def get_data_wifi_month(model,i,collection,station):
    #start_time = db_start_time
    #end_time = db_end_time
    date_format_str='%Y-%m-%dT%H:%M:%S.%fZ'
    
    #da = pd.DataFrame(columns = ['Start time' , 'End time', 'Total MAC' , 'MAC PASS','MAC FAIL','Retry FAIL'])
    #while  start_time < db_end_time :
        #end_time =  start_time.replace(minute=0,second=0,microsecond=0) + timedelta(hours = 12)
    df=pd.DataFrame(list(collection.find({"Time":{"$gte":db_start_time,"$lt":db_end_time}})))
    #df.to_csv('%s_%s.csv'%(station,i))
    if 'Empty DataFrame' not in str(df.head()) :
        time1 =  df.iloc[0]["Time"] 
        time2=df.iloc[-1]["Time"] 
        start = pd.to_datetime(time1, format=date_format_str)
        end = pd.to_datetime(time2, format=date_format_str)

        if df["Result"].count() != 0:
              des= df["Result"].describe()                    # des[count:,unique:,top:,freq:]
              if  des[1] == 1 :
                      if  des[2]=="FAIL": p,f,mac=0,df.Result.value_counts().FAIL,des[0]
                      else: f,p,mac=0,df.Result.value_counts().PASS,des[0] 
              else: 
                  #p = df.Result.value_counts().PASS        # df["Result"].value_counts().PASS      Count "PASS" value in pandas ----
                  dp = df[df["Result"]=='PASS']
                  p = dp['MAC'].nunique()
                  f = df.Result.value_counts().FAIL
                  mac = df['MAC'].nunique()
                  #print ( df["MAC"].describe())  
    else: 
        p=0
        f= 0
        mac = 0
    da.loc[i] = [model,station,str(start).split('.')[0],str(end).split('.')[0],mac,p,(mac-p),f]
    #return  str(start).split('.')[0],str(end).split('.')[0], mac,p,f
    return da.loc[i]
    #start_time = end_time
    #print ('---------------------------------%s-----------------------------------------'%i)
    #i += 1
    #print(da)
    #da.to_csv('%s%s_%s_%s %s.csv'%(datapath_m,model,station,str(db_start_time).split('.')[0].split(' ')[0],str(db_end_time).split('.')[0].split(' ')[0]))
    #da.to_csv('%s%s_%s.csv'%(datapath_m,model,station))
    #print ('Get data %s %s OK'%(Model,station) )




#####   MAIN  #---------------------------------------------------------------------------------------------------

'''
get_data_wifi(Model,collection_AM,station='AM')
get_data_cable(Model,collection_AT,station='AT')    
get_data_wifi(Model,collection_AW,station='AW') '''   
i=0
da = pd.DataFrame(columns = ['Model','Station','Start time' , 'End time', 'Total MAC' , 'MAC PASS','MAC FAIL','Retry FAIL'])
for Model in Model_list   :                          #'CGNV5_PRO_MGC'
    #conn = MongoClient(mongoinfo)
    conn = MongoClient("172.18.136.250:27017")
    db = conn[Model]
    collection_AM = db['Auto_T0']
    collection_AT = db['Auto_PCBA_AUTOMATION']
    collection_AW= db['Auto_W11']
    #get_data_wifi_month(Model,collection_AW,station='AW')
    #get_data_cable_month(Model,collection_AT,station='AT')
    #get_data_wifi_month(Model,collection_AM,station='AM')
    #i=0
    
    get_data_wifi_month(Model,i,collection_AM,station='AM')
    i += 1
    get_data_cable_month(Model,i,collection_AT,station='AT')
    i += 1
    get_data_wifi_month(Model,i,collection_AW,station='AW')
    i += 1
    
    
    #da.to_csv('%s%s_%s.csv'%(datapath_m,model,station))

    print ('Export data %s OK'%(Model) )
#print (da)
da.to_csv('%sMonth_%s.csv'%(datapath_m,str(db_start_time).split('.')[0].split(' ')[0].split('-')[1]))