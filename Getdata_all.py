#import numpy as np 
from pymongo import MongoClient
import os,sys,time,datetime,bz2,re
import pandas as pd

sys.path.append('.\\config\\')
with open('.\\config\\Config.ini') as f:
    exec(f.read())


user = 'VNMonitor'
pwd = 'Monitor'
DB_ip = '172.18.138.250'
DB_port = 27017
mongoinfo = "mongodb://"+user+":"+pwd+"@"+str(DB_ip)+":"+str(DB_port)+"/"


def Getdata_autow11(Model,collection,start_time,end_time):
    df=pd.DataFrame(list(collection.find({"Time":{"$gte":start_time,"$lt":end_time}})))
    print (df.tail())
    df=df.drop(['Log'],axis=1)         # delete Log column
    df["Descrip"] = df["Issue"].str.extract(regex_w11)
    df["Descrip_"] = df["Issue"].str.extract(regex_w11_)
    df.to_csv('%sAW1_%s.csv'%(datapath,Model))
    print (' Export AW1 OK')
    
def Getdata_autoam(Model,collection,start_time,end_time):
    df=pd.DataFrame(list(collection.find({"Time":{"$gte":start_time,"$lt":end_time}})))
    df=df.drop(['Log'],axis=1)         # delete Log column
    df["Descrip"] = df["Issue"].str.extract(regex_am)
    #df["Descrip_"] = df["Issue"].str.extract(regex_w11_)
    df.to_csv('%sAM1_%s.csv'%(datapath,Model))
    print (' Export AM1 OK')

def Getdata_autocable(Model,collection,start_time,end_time):
    df=pd.DataFrame(list(collection.find({"end_time":{"$gte":start_time,"$lt":end_time}})))
    df=df.drop(['Log'],axis=1)         # delete Log column
    df['Des'] = [x[1] if 'ER' in str(x) else "".join(re.split("\W+",str(re.findall("\w+\d+",str(x).split(',')[0])))) for x in df["msg"].str.findall(regex_cable) ]
    df['Des_'] =[ ErrorCode_Cable[str(x)] if str(x) in ErrorCode_Cable else (''.join(re.split("\W+", str(x)))) for x in df['Des'] ]
    df.to_csv('%sAT1_%s.csv'%(datapath,Model))
    print (' Export AT1 OK')
    
    
def count_rxfail(collection):
    count = 0
    real_count = 0
    x=[]
    for i in collection.find({"Time":{"$gte":db_start_time,"$lt":db_end_time},"Result":"FAIL","Issue":"ErrorCode(TDF0000): Unkown Error"}):
        f= bz2.decompress(i['Log'])
        if b"--- [Failed]" in f :
            rx_f= f.split(b"--- [Failed]")[0].split(b'\n')[-18]
            if rx_f in err_type :
                #print(i["MAC"])
                x.append(i["MAC"])
                print(i["MAC"])
                count += 1

    #mac_list = pd.unique(x).tolist()      #remove duplicated MAC in x
    f=pd.DataFrame(x)
    s=f.drop_duplicates(keep='last')       #remove duplicated MAC in x  (other_way)
    s.to_csv("RX_MAC_Fail.csv")
    print(COLORS["Y"],"\t RX FAIL Count :\t %s \t "%count,"% "," RX MAC FAIL: %s"%len(s))
 

#conn = MongoClient("127.0.0.1:27017")
#conn = MongoClient("172.18.136.250:27017")
conn = MongoClient(mongoinfo)


######138.250######
#db = conn['CGNV5_TFC']
#db = conn['CGN5_AP']
#db = conn['CGNV5_UNE']
#db = conn['CGNV5_CLR']
#db = conn['CHITA_HUB5']
#db = conn['CODA-5712']



#MAin

#---------------------------------------------------------------------------------------------------------------------
db_start_time = datetime.datetime(2022,1,1,8,0)      #Set time begin(yyyy,mm,dd,hh,mm)
db_end_time = datetime.datetime(2022,12,10,20,0) 
#---------------------------------------------------------------------------------------------------------------------

Model_list = ['CGN5-AP2','CGNV5_PRO_MGC','CGNV5_UNE','CGNV5_TF2','CGN5_AP','CODA-5712','CHITA_HUB5'] 
#Model_list = ['CGNV5_PRO_MGC']
#---------------------------------------------------------------------------------------------------------------------




for Model in Model_list   :   

      db = conn[Model]

      collection_T0 = db['Auto_T0']
      collection_wifi = db['Auto_W11']
      collection_cable = db['Auto_PCBA_AUTOMATION']
      
      
      
      Getdata_autow11(Model,collection_wifi,db_start_time,db_end_time)
      Getdata_autocable(Model,collection_cable,db_start_time,db_end_time)
      Getdata_autoam(Model,collection_T0,db_start_time,db_end_time)
      print ('---------------------DONE------------------------')
