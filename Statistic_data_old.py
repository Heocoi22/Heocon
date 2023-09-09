import os,datetime,time
import pandas as pd
import pymongo
from pymongo import MongoClient
from datetime import timedelta, time as _time

#---------------------------------------------------------------------------------------------------------------------
user = 'VNMonitor'
pwd = 'Monitor'
DB_ip = '172.18.138.250'
DB_port = 27017
mongoinfo = "mongodb://"+user+":"+pwd+"@"+str(DB_ip)+":"+str(DB_port)+"/"
datapath = '.\\Data\\'  
 #---------------------------------------------------------------------------------------------------------------------
db_start_time = datetime.datetime(2022,12,12,8,0)      #Set time begin(yyyy,mm,dd,hh,mm)
db_end_time = datetime.datetime(2022,12,17,20,0) 
#---------------------------------------------------------------------------------------------------------------------

#Model = 'CGN5-AP2'
#Model = 'CGNV5_PRO_MGC'
#Model = 'CODA-5712'
Model = 'CGNV5_TF2'
#---------------------------------------------------------------------------------------------------------------------
conn = MongoClient(mongoinfo)
db = conn[Model]
collection_AM = db['Auto_T0']
collection_AT = db['Auto_PCBA_AUTOMATION']
collection_AW= db['Auto_W11']
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

#####   MAIN  #---------------------------------------------------------------------------------------------------


get_data_wifi(Model,collection_AM,station='AM')
get_data_cable(Model,collection_AT,station='AT')    
get_data_wifi(Model,collection_AW,station='AW')         