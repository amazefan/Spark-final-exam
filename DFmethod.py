# -*- coding: utf-8 -*-
"""
Created on Fri May 18 09:31:04 2018

@author: Administrator
"""

#import sql module to convey data into dataframe
#Dataframe is easy to sql
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
spark = SparkSession.builder.appName("dataFrameApply").getOrCreate()
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
#StructType:a list of column names,the type of each column will be inferred from data.
schema = StructType([
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
CALL_DF_CNT = spark.sql(" SELECT id,COUNT(id) AS call_cnt FROM voice GROUP BY id")
CALL_DF_OUT = spark.sql(" SELECT id,COUNT(id) AS call_out FROM voice WHERE in_out = '0' GROUP BY id")
CALL_DF_IN = spark.sql(" SELECT id,COUNT(id) AS call_in FROM voice WHERE in_out = '1' GROUP BY id")
CALL_DF_TIME = spark.sql("SELECT id,SUM(time) as time from (SELECT id,CAST(end_time as int)-CAST(start_time as int) AS time FROM voice) GROUP BY id")