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

db_start_time = datetime.datetime(2023,4,25,8,0)      #Set time begin(yyyy,mm,dd,hh,mm)
db_end_time = datetime.datetime(2023,4,25,20,0) 

#start_time = datetime.datetime(2022,7,23,8,0)      #Set time begin(yyyy,mm,dd,hh,mm)
#end_time = datetime.datetime(2022,7,23,20,0) 

def Getdata_autow11(collection,start_time,end_time):
    df=pd.DataFrame(list(collection.find({"Time":{"$gte":start_time,"$lt":end_time}})))
    #df["Descrip"] = df.apply(lambda row: splitererr(bz2.decompress(row.Log)) if b'[Failed]' in bz2.decompress(row.logfile) else "None", axis = 1)
    df["Error_Log"] = df.apply(lambda row: splitererr(bz2.decompress(row.Log)) if 'FAIL' in row.Result else "", axis = 1)
    df=df.drop(['Log'],axis=1)         # delete Log column
    df["Descrip"] = df["Issue"].str.extract(regex_w11)
    df["Descrip_"] = df["Issue"].str.extract(regex_w11_)
    df.to_csv('%sAW1_%s.csv'%(datapath,Model))
    print ('Export AW1 OK')
    
def Getdata_autoam(collection,start_time,end_time):
    df=pd.DataFrame(list(collection.find({"Time":{"$gte":start_time,"$lt":end_time}})))
    df=df.drop(['Log'],axis=1)         # delete Log column
    df["Descrip"] = df["Issue"].str.extract(regex_am)
    #df["Descrip_"] = df["Issue"].str.extract(regex_w11_)
    df.to_csv('%sAM1_%s.csv'%(datapath,Model))
    print ('Export AM1 OK')

def Getdata_autocable(collection,start_time,end_time):
    df=pd.DataFrame(list(collection.find({"end_time":{"$gte":start_time,"$lt":end_time}})))
    df=df.drop(['Log'],axis=1)         # delete Log column
    df['Des'] = [x[1] if 'ER' in str(x) else "".join(re.split("\W+",str(re.findall("\w+\d+",str(x).split(',')[0])))) for x in df["msg"].str.findall(regex_cable) ]
    df['Des_'] =[ ErrorCode_Cable[str(x)] if str(x) in ErrorCode_Cable else (''.join(re.split("\W+", str(x)))) for x in df['Des'] ]
    df.to_csv('%sAT1_%s.csv'%(datapath,Model))
    print ('Export AT1 OK')
    
def get_cpk(collection,start_time,end_time) :

    j=2
    da = pd.DataFrame(columns = ['MAC' ,'Result','Area'])
    for i in collection.find({"start_time":{"$gte":start_time,"$lt":end_time},'result': "PASS"}):

        f= bz2.decompress(i['Log'])    

        mac = i['mac']
        result = i['result']
        area = i['StationName']
        da.loc[0,'MAC'] = 'Min'
        da.loc[1,'MAC'] = 'Max'
        da.loc[j,'MAC'] = mac
        da.loc[j,'Result'] = result
        da.loc[j,'Area'] = area
        flogfile_data = f.decode().split('\n')

        count = 0
        for ftest_item  in ftest_list :

                # print "test_item:  "+ftest_item[1]              
                while 1:

                    if count >= len(flogfile_data): 
                        #print ("error : %s "%(ftest_item[1]))
                        #print (count) 
                        count = 0
                        break

                    if ftest_item[1] in flogfile_data[count]:                        
                        if "Voice" in ftest_item[0]:
                            ftest_value = flogfile_data[count].split(ftest_item[1])[-1].split('(mA):')[0].split()[0].strip()
                            #print (ftest_value)
                            ftest_limit = flogfile_data[count].split(ftest_item[1])[-1].split('(')[-1].split(')')[0].strip().split('~')               
                            #print (ftest_value)
                            #print (ftest_limit)
                            count += 1
                            break                    
                        elif "Time" in ftest_item[1] or  "Result" in ftest_item[1]:
                            if count <= len(flogfile_data)-5:   
                                count +=1
                                continue
                            else:    
                                #print ("count:" + str(count) + "len:" + str(len(flogfile_data)))
                                #print (flogfile_data[count])
                                ftest_value = flogfile_data[count].split(ftest_item[1])[-1].split('(')[0].split()[0].strip()
                                ftest_limit = flogfile_data[count].split(ftest_item[1])[-1].split('(')[-1].split(')')[0].strip().split('~')  
                                #print (ftest_value)
                                #print (ftest_limit)
                                if len(ftest_limit) == 2 :
                                    da.loc[0,ftest_item[0]] = ftest_limit[0]
                                    da.loc[1,ftest_item[0]] = ftest_limit[1]
                                da.loc[j,ftest_item[0]] = ftest_value
                                count = 0
                                break
                        else:    
                            #print ("count:" + str(count) + "len:" + str(len(flogfile_data)))
                            #print (flogfile_data[count])
                            ftest_value = flogfile_data[count].split(ftest_item[1])[-1].split('(')[0].split()[0].strip()
                            ftest_limit = flogfile_data[count].split(ftest_item[1])[-1].split('(')[-1].split(')')[0].strip().split('~')               
                            #print (ftest_value) 
                            #print (ftest_limit)
                            if len(ftest_limit) == 2 :
                                da.loc[0,ftest_item[0]] = ftest_limit[0]
                                da.loc[1,ftest_item[0]] = ftest_limit[1]
                            da.loc[j,ftest_item[0]] = ftest_value

                            count += 1
                            break

                    count += 1
        j += 1


    da.to_csv('%s %s_Cpk.csv'%(datapath,Model))
    print ( 'Get CPK OK')





    
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

def get_testtime_telnetfail(collection) :

    j=0
    da = pd.DataFrame(columns = ['MAC' , 'Testtime'])
    #ErrorCode(TDFC100):FAIL:192.168.100.2 Telnet login Failure
    for i in collection.find({"Time":{"$gte":db_start_time,"$lt":db_end_time},'Issue':'ErrorCode(TDFC100):FAIL:192.168.100.2 Telnet login Failure'}):
        #print (i)
    #df=pd.DataFrame(list(collection.find({"Time":{"$gte":start_time,"$lt":end_time},"Result":"FAIL",'MAC':'DC360CD99E30'})))
        f= bz2.decompress(i['Log'])
        mac = i["MAC"]
        #print (mac)
        if b"192.168.100.2 ping success" in f :
            #tf = f.split(b"(sec)")[1].split(b":")[-1]
            tf = f.split(b"(sec)")[1].split(b":")[1]
            tf1= (str(tf).split(' ')[1])
            da.loc[j] = [mac,tf1]
            j=j+1

    da.to_csv('%sTesttimetelnetfail%s.csv'%(datapath,Model))        

def get_issue_from_log_aw(collection)  :
    j=0
    da = pd.DataFrame(columns = ['MAC' ,'Result','Area','Issue','Descrip'])
   
    #for i in collection.find({"Time":{"$gte":start_time,"$lt":end_time},'Result': "FAIL",'MAC':'DC360CD9F740'}):
    for i in collection.find({"Time":{"$gte":db_start_time,"$lt":db_end_time},'Result': "FAIL"}):
            des = re.findall(regex_w11,i['Issue'])
            f= bz2.decompress(i['Log'])
            #print(f)
            if b"*************" in f :
                #print (f)
                tf = f.split(b"*************")[-1].split(b"[Failed]")[0].split(b' s')[-2].split(b":")[0]
                msg = " ".join(re.split("\s+", str(tf).split('n')[1].split("'")[0], flags=re.UNICODE))
                da.loc[j] = [i['MAC'],i['Result'],i["Area_"],msg,des]
            j = j+1    
    
    da.to_csv('%s_Issue_log_%s.csv'%(datapath,Model)) 
    print ("---------End----------")

def splitererr(a):
    if  b"*************" in a : 
        b = a.split(b"*************")[-1].split(b"[Failed]")[0].split(b' s')[-2].split(b":")[0]
        if "   " in str(b):
            b = " ".join(re.split("\s+", str(b).split('n')[1].split("'")[0], flags=re.UNICODE))
    elif b"ErrorCode" in a :  
        b = str(a).split('Check DUT')[0].split(':')[-1] 
        #b = ''.join(b.splitlines())    
    return b

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

db = conn[Model]

collection_T0 = db['Auto_T0']
collection_wifi = db['Auto_W11']
collection_cable = db['Auto_PCBA_AUTOMATION']

#MAin

#---------------------------------------------------------------------------------------------------------------------
#db_start_time = datetime.datetime(2023,3,10,8,0)      #Set time begin(yyyy,mm,dd,hh,mm)
#db_end_time = datetime.datetime(2023,3,16,20,0) 
#---------------------------------------------------------------------------------------------------------------------


#Getdata_autow11(collection_wifi,db_start_time,db_end_time)
Getdata_autocable(collection_cable,db_start_time,db_end_time)
#Getdata_autoam(collection_T0,db_start_time,db_end_time)
#get_issue_from_log_aw(collection_wifi)
#count_rxfail(collection_wifi)                      # CODA 5XXX
#get_cpk(collection_cable,db_start_time,db_end_time)



'''
import pyodbc,bz2,re
import pandas as pd

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = '172.18.130.11' 
database = 'test' 
username = 'test' 
password = 'test'  
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
# select 26 rows from SQL table to insert in dataframe.
#query = "SELECT [CountryRegionCode], [Name] FROM Person.CountryRegion;"

#query = "select mac,status,testtime, from TESTlog where testtime BETWEEN '2022-12-10 00:00:00' AND '2022-12-10 23:00:00' AND PN = '1610100002VD' "

query = "SELECT [mac], [status], [testtime],[logfile], [StationID], [ErrorCode], [ErrorMsg]  FROM testlog where station = 'W1-1'and Modelname = 'CGNV5 TFC' and testtime BETWEEN '2023-1-9 08:00:00' AND '2023-1-9 23:00:00'"

#query = "SELECT * FROM testlog where testtime BETWEEN '2022-12-10 12:00:00' AND '2022-12-10 23:00:00'"

df = pd.read_sql(query, cnxn)
des= df["logfile"].describe()
#print (des)
#for i in range(0,des[1]) :
#    df["Descrip"][i] = bz2.decompress(df['logfile'][i])
#des_ = bz2.BZ2Decompressor()
#df["Descrip"] = des_.decompress(df["logfile"])
#print (bz2.decompress(df['logfile'][0]))

def splitererr(a):
    #b =a
    #b = a.split(b"*************")[-1].split(b"[Failed]")[0].split(b' s')[-2].split(b":")[0]
    b = a.split(b"*************")[-1].split(b"[Failed]")[0].split(b' s')[-2].split(b":")[0]
    b = " ".join(re.split("\s+", str(b).split('n')[1].split("'")[0], flags=re.UNICODE))
    return b
#df["Descrip"] = df.apply(lambda row: bz2.decompress(row.logfile).split(b"*************")[-1].split(b"[Failed]")[0].split(b' s')[-2].split(b":")[0], axis = 1)
df["Descrip"] = df.apply(lambda row: splitererr(bz2.decompress(row.logfile)) if b'[Failed]' in bz2.decompress(row.logfile) else "None", axis = 1)
print (df['Descrip'][0])
df=df.drop(['logfile'],axis=1) 
#print (df['Descrip'][0].split(b"*************")[-1].split(b"[Failed]")[0].split(b' s')[-2].split(b":")[0] )
#detail_fail = df.groupby(["StationID","ErrorCode"])[["ErrorCode"]].count()
#detail_fail = df.groupby(["StationID","status"])[["status"]].count()
#print (detail_fail)
#print(df.head(26))
#print (df.tail())
#print (df.describe())

df.to_csv("D:\\testlog_manual.csv") '''