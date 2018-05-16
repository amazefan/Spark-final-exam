# -*- coding: utf-8 -*-
"""
Created on Mon May 14 21:15:40 2018

@author: Administrator
"""

import pyspark
from pyspark import SparkContext
from operator import add
path_voice = r'E:\python.file\structure data\JDATA\voice_train.txt'
path_uid = r'E:\python.file\structure data\JDATA\uid_train.txt'
path_sms = r'E:\python.file\structure data\JDATA\sms_train.txt'
path_wa = r'E:\python.file\structure data\JDATA\wa_train.txt'


#create a spark shell to load .txt data
sc = SparkContext()
voice = sc.textFile(path_voice)
uid = sc.textFile(path_uid)
sms = sc.textFile(path_sms)
wa = sc.textFile(path_wa)
mode = lambda x: x.split('\t')
voicedata = voice.map(mode)
uiddata = uid.map(mode)
smsdata = sms.map(mode)
wadata = wa.map(mode)

#RDD method to derivate variable
def flatten(Iterator):
    for item in Iterator:
        if isinstance(item,(list,tuple)):
            for sub_item in flatten(item):
                yield sub_item
        else:yield item
            
funcmode1 = lambda x:(x[0],1)
funcmode2 = lambda x,y:x+y
call_type = {'LOCAL':1,'IN_PROVINCE':2,'BY_PROVINCE':3,'GAT':4,'INTERNATIONAL':5}
for type_ in call_type:
    exec("CALL_RDD_{} = voicedata.filter(lambda x:x[6]=='{}').map(funcmode1).reduceByKey(funcmode2)".format(type_,str(call_type[type_])))
CALL_RDD_CNT = voicedata.map(funcmode1).reduceByKey(funcmode2)   
CALL_RDD_IN = voicedata.filter(lambda x:x[7]=='1').map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_OUT = voicedata.filter(lambda x:x[7]=='0').map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_TIME = voicedata.map(lambda x:(x[0],int(x[5])-int(x[4]))).reduceByKey(funcmode2)
CALL_RDD_PERSON = voicedata.filter(lambda x:x[3]=='11').map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_BUSSINESS = voicedata.filter(lambda x:x[3]!='11').map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_HOWMANYPERSON = voicedata.filter(lambda x:x[3]=='11').map(lambda x:(x[0],x[1])).distinct().map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_HOWMANYBUSINESS = voicedata.filter(lambda x:x[3]!='11').map(lambda x:(x[0],x[1])).distinct().map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_AVGTIME = voicedata.map(lambda x:(x[0],(int(x[5])-int(x[4]),1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],x[1][0]/x[1][1]))
CALL_RDD_DAYTIME = voicedata.filter(lambda x:int(x[4][2:4])>=7 and int(x[4][2:4])<=12).map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_AFTERNOON = voicedata.filter(lambda x:int(x[4][2:4])>=12 and int(x[4][2:4])<=18).map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_NIGHT = voicedata.filter(lambda x:int(x[4][2:4])>=18 and int(x[4][2:4])<=24).map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_BEFOREDAWN = voicedata.filter(lambda x:int(x[4][2:4])>=0 and int(x[4][2:4])<=7).map(funcmode1).reduceByKey(funcmode2)
CALL_RDD_MAXTIME = voicedata.map(lambda x:(x[0],int(x[5])-int(x[4]))).reduceByKey(lambda x,y:max(x,y))
CALL_RDD_MINTIME = voicedata.map(lambda x:(x[0],int(x[5])-int(x[4]))).reduceByKey(lambda x,y:min(x,y))
CALL_RDD_TIMEVAR = voicedata.map(lambda x:(x[0],int(x[5])-int(x[4]))).join(CALL_RDD_AVGTIME).join(CALL_RDD_CNT).map(lambda x:(x[0],tuple(flatten(x[1])))).map(lambda x:(x[0],(x[1][0]-x[1][1])**2/x[1][2])).reduceByKey(funcmode2)
CALL_RDD_INOUTRATIO = CALL_RDD_IN.join(CALL_RDD_CNT).map(lambda x:(x[0],x[1][0]/x[1][1]))
 
#import sql module to convey data into dataframe
#Dataframe is easy to sql
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession.builder.appName("dataFrameApply").getOrCreate()
#StructType:a list of column names,the type of each column will be inferred from data.
schema = schema = StructType([
    StructField("id",StringType(),True),
    StructField("opp_num",StringType(),True),
    StructField("opp_head",StringType(),True),
    StructField("opp_len",StringType(),True),
    StructField("start_time",StringType(),True),
    StructField("end_time",StringType(),True),
    StructField("call_type",StringType(),True),
    StructField("in_out",StringType(),True)
])
#create a dataframe based on pyspark
#register this df,so sql can be done on it(named voice)
voiceDF = spark.createDataFrame(voicedata,schema)
voiceDF.registerTempTable("voice")

#Variable Derivation(VOICE) on DF
#def Derivation_call_type():
call_type = {'LOCAL':1,'IN_PROVINCE':2,'BY_PROVINCE':3,'GAT':4,'INTERNATIONAL':5}
for type_ in call_type:
    sql_call = r" SELECT id,COUNT(id) AS call_{} FROM voice WHERE call_type= '{}' GROUP BY id".format(str(call_type[type_]),str(call_type[type_]))
    exec('CALL_{} = spark.sql("{}")'.format(type_,sql_call))
CALL_CNT = spark.sql(" SELECT id,COUNT(id) AS call_cnt FROM voice GROUP BY id")
CALL_OUT = spark.sql(" SELECT id,COUNT(id) AS call_out FROM voice WHERE in_out = '0' GROUP BY id")
CALL_IN = spark.sql(" SELECT id,COUNT(id) AS call_in FROM voice WHERE in_out = '1' GROUP BY id")
CALL_TIME = spark.sql("SELECT id,SUM(time) as time from (SELECT id,CAST(end_time as int)-CAST(start_time as int) AS time FROM voice) GROUP BY id")






