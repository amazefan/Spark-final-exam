# -*- coding: utf-8 -*-
"""
Created on Sun May 27 14:02:16 2018

@author: 98486
"""

import pyspark
from pyspark import SparkContext
from operator import add
import os
os.chdir(r'C:\Users\98486\Desktop\风险用户识别\data')
path_voice = 'voice_train.txt'
path_uid = 'uid_train.txt'
path_sms = 'sms_train.txt'
path_wa = 'wa_train.txt'

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
funcmode1 = lambda x:(x[0],1)
funcmode2 = lambda x,y:x+y
funcmode3 = lambda x:(x[1],1)
times=lambda x:(x[0],int(x[2]))
time=lambda x:(x[0],int(x[3]))
up=lambda x:(x[0],int(x[4]))
down=lambda x:(x[0],int(x[5]))

def flatten(Iterator):
    for item in Iterator:
        if isinstance(item,(list,tuple)):
            for sub_item in flatten(item):
                yield sub_item
        else:yield item

def fillna(a):
    for i in range(len(a)):
        if a[i]==None:
            a[i]=0
    return a

#将APP和网站分开，然后取>200的类型
APP=wadata.filter(lambda x:x[6]=='1').map(funcmode3).reduceByKey(funcmode2).filter(lambda x: x[1]>200).collect()
import pandas as pd
APP=pd.DataFrame(APP)
APP.to_csv('APP.csv',encoding='gbk')



##网站信息
WA_WEB_TIMES=wadata.filter(lambda x:x[6]=='0').map(times).reduceByKey(funcmode2)
WA_WEB_TIMESA=wadata.filter(lambda x:x[6]=='0').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_WEB_TIMESV=wadata.filter(lambda x:x[6]=='0' and x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_WEB_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_WEB_TIME=wadata.filter(lambda x:x[6]=='0').map(time).reduceByKey(funcmode2)
WA_WEB_TIMEA=wadata.filter(lambda x:x[6]=='0').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_WEB_TIMEV=wadata.filter(lambda x:x[6]=='0' and x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_WEB_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_WEB_BU=wadata.filter(lambda x:x[6]=='0').map(up).reduceByKey(funcmode2)
WA_WEB_BUA=wadata.filter(lambda x:x[6]=='0').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_WEB_BUV=wadata.filter(lambda x:x[6]=='0' and x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_WEB_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_WEB_BD=wadata.filter(lambda x:x[6]=='0').map(down).reduceByKey(funcmode2)
WA_WEB_BDA=wadata.filter(lambda x:x[6]=='0').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_WEB_BDV=wadata.filter(lambda x:x[6]=='0' and x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_WEB_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_WEB_RATIO=wadata.filter(lambda x:x[6]=='0' and x[4]!='NULL' and x[5]!='NULL').map(lambda x:(x[0],(int(x[4]),int(x[5])))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],int(x[1][0])/(int(x[1][1])+0.0001)))
#WA_WEB_RATIO1=WA_WEB_BU.join(WA_WEB_BD).map(lambda x:(int(x[1][0])/int(x[1][1])))

##RDD转为DF，指定模式
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("dataFrameApply").getOrCreate()
WEB=WA_WEB_TIMES.fullOuterJoin(WA_WEB_TIMESA).fullOuterJoin(WA_WEB_TIMESV).fullOuterJoin(WA_WEB_TIME).fullOuterJoin(WA_WEB_TIMEA).fullOuterJoin(WA_WEB_TIMEV).fullOuterJoin(WA_WEB_BU).fullOuterJoin(WA_WEB_BUA).fullOuterJoin(WA_WEB_BUV).fullOuterJoin(WA_WEB_BD).fullOuterJoin(WA_WEB_BDA).fullOuterJoin(WA_WEB_BDV).fullOuterJoin(WA_WEB_RATIO).map(lambda x:(x[0],tuple(flatten(x[1])))).map(lambda x:(list(flatten(x))))
schema1=StructType([
    StructField("id", StringType(), True),
    StructField("WA_WEB_TIMES", LongType(), True),
    StructField("WA_WEB_TIMESA", DoubleType(), True),
    StructField("WA_WEB_TIMESV", DoubleType(), True),
    StructField("WA_WEB_TIME", LongType(), True),
    StructField("WA_WEB_TIMEA", DoubleType(), True),
    StructField("WA_WEB_TIMEV", DoubleType(), True),
    StructField("WA_WEB_BU", LongType(), True),
    StructField("WA_WEB_BUA", DoubleType(), True),
    StructField("WA_WEB_BUV", DoubleType(), True),
    StructField("WA_WEB_BD", LongType(), True),
    StructField("WA_WEB_BDA", DoubleType(), True),
    StructField("WA_WEB_BDV", DoubleType(), True),
    StructField("WA_WEB_RATIO", DoubleType(), True)
])    
WEBDF=spark.createDataFrame(WEB,schema1)
WEBDF.toPandas().to_csv('df\\WEBDF.csv')


#APP信息
WA_APP_TIMES=wadata.filter(lambda x:x[6]=='1').map(times).reduceByKey(funcmode2)
WA_APP_TIMESA=wadata.filter(lambda x:x[6]=='1').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_APP_TIMESV=wadata.filter(lambda x:x[6]=='1' and x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_APP_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_APP_TIME=wadata.filter(lambda x:x[6]=='1').map(time).reduceByKey(funcmode2)
WA_APP_TIMEA=wadata.filter(lambda x:x[6]=='1').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_APP_TIMEV=wadata.filter(lambda x:x[6]=='1' and x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_APP_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_APP_BU=wadata.filter(lambda x:x[6]=='1').map(up).reduceByKey(funcmode2)
WA_APP_BUA=wadata.filter(lambda x:x[6]=='1').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_APP_BUV=wadata.filter(lambda x:x[6]=='1' and x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_APP_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_APP_BD=wadata.filter(lambda x:x[6]=='1').map(down).reduceByKey(funcmode2)
WA_APP_BDA=wadata.filter(lambda x:x[6]=='1').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_APP_BDV=wadata.filter(lambda x:x[6]=='1' and x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_APP_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_APP_RATIO=wadata.filter(lambda x:x[6]=='1'and x[4]!='NULL' and x[5]!='NULL').map(lambda x:(x[0],(int(x[4]),int(x[5])))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],int(x[1][0])/(int(x[1][1])+0.0001)))
(WA_APP_TIMES.count(),WA_APP_TIMESA.count(),WA_APP_TIMESV.count(),WA_APP_TIME.count(),WA_APP_TIMES.count(),WA_APP_TIMEA.count(),WA_APP_TIMEV.count(),WA_APP_BU.count(),WA_APP_BUA.count(),WA_APP_BUV.count(),WA_APP_BD.count(),WA_APP_BDA.count(),WA_APP_BDV.count(),WA_APP_RATIO.count()) 

##RDD转为DF，指定模式
APP=WA_APP_TIMES.fullOuterJoin(WA_APP_TIMESA).fullOuterJoin(WA_APP_TIMESV).fullOuterJoin(WA_APP_TIME).fullOuterJoin(WA_APP_TIMEA).fullOuterJoin(WA_APP_TIMEV).fullOuterJoin(WA_APP_BU).fullOuterJoin(WA_APP_BUA).fullOuterJoin(WA_APP_BUV).fullOuterJoin(WA_APP_BD).fullOuterJoin(WA_APP_BDA).fullOuterJoin(WA_APP_BDV).fullOuterJoin(WA_APP_RATIO).map(lambda x:(tuple(flatten(x))))
schema2=StructType([
        StructField('id',StringType(),True),
        StructField('WA_APP_TIMES',LongType(),True),
        StructField('WA_APP_TIMESA',FloatType(),True),
        StructField('WA_APP_TIMESV',FloatType(),True),
        StructField('WA_APP_TIME',LongType(),True),
        StructField('WA_APP_TIMEA',FloatType(),True),
        StructField('WA_APP_TIMEV',FloatType(),True),
        StructField('WA_APP_BU',LongType(),True),
        StructField('WA_APP_BUA',FloatType(),True),
        StructField('WA_APP_BUV',FloatType(),True),
        StructField('WA_APP_BD',LongType(),True),
        StructField('WA_APP_BDA',FloatType(),True),
        StructField('WA_APP_BDV',FloatType(),True),
        StructField('WA_APP_RATIO',FloatType(),True)   
]) 
APPDF=spark.createDataFrame(APP,schema2)
APPDF.toPandas().to_csv('df\\APPDF.csv')


##总表信息
WA_TIMES_RATIO=WA_WEB_TIMES.fullOuterJoin(WA_APP_TIMES).mapValues(list).mapValues(lambda x:fillna(x)).map(lambda x:(x[0],int(x[1][0])/(int(x[1][1])+0.0001)))
WA_TIMES=wadata.filter(lambda x:x[2]!='NULL').map(times).reduceByKey(funcmode2)
WA_TIMESA=wadata.filter(lambda x:x[2]!='NULL').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_TIMESV=wadata.filter(lambda x:x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_TIME_RATIO=WA_WEB_TIME.fullOuterJoin(WA_APP_TIME).mapValues(list).mapValues(lambda x:fillna(x)).map(lambda x:(x[0],int(x[1][0])/(int(x[1][1])+0.0001)))
WA_TIME=wadata.filter(lambda x:x[3]!='NULL').map(time).reduceByKey(funcmode2)
WA_TIMEA=wadata.filter(lambda x:x[3]!='NULL').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_TIMEV=wadata.filter(lambda x:x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_BU_RATIO=WA_WEB_BU.fullOuterJoin(WA_APP_BU).mapValues(list).mapValues(lambda x:fillna(x)).map(lambda x:(x[0],int(x[1][0])/(int(x[1][1])+0.0001)))
WA_BU=wadata.filter(lambda x:x[4]!='NULL').map(up).reduceByKey(funcmode2)
WA_BUA=wadata.filter(lambda x:x[4]!='NULL').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_BUV=wadata.filter(lambda x:x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_BD_RATIO=WA_WEB_BD.fullOuterJoin(WA_APP_BD).mapValues(list).mapValues(lambda x:fillna(x)).map(lambda x:(x[0],int(x[1][0])/(int(x[1][1])+0.0001)))
WA_BD=wadata.filter(lambda x:x[5]!='NULL').map(down).reduceByKey(funcmode2)
WA_BDA=wadata.filter(lambda x:x[5]!='NULL').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_BDV=wadata.filter(lambda x:x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_RATIO=wadata.filter(lambda x:x[4]!='NULL' and x[5]!='NULL').map(lambda x:(x[0],(int(x[4]),int(x[5])))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],int(x[1][0])/(int(x[1][1])+0.0001)))
(WA_TIMES_RATIO.count(),WA_TIMES.count(),WA_TIMESA.count(),WA_TIMESV.count(),WA_TIME_RATIO.count(),WA_TIME.count(),WA_TIMEA.count(),WA_TIMEV.count(),WA_BU_RATIO.count(),WA_BU.count(),WA_BUA.count(),WA_BUV.count(),WA_BD_RATIO.count(),WA_BD.count(),WA_BDA.count(),WA_BDV.count(),WA_RATIO.count())

##RDD转为DF，指定模式
TOTAL=WA_TIMES_RATIO.fullOuterJoin(WA_TIMES).fullOuterJoin(WA_TIMESA).fullOuterJoin(WA_TIMESV).fullOuterJoin(WA_TIME_RATIO).fullOuterJoin(WA_TIME).fullOuterJoin(WA_TIMEA).fullOuterJoin(WA_TIMEV).fullOuterJoin(WA_BU_RATIO).fullOuterJoin(WA_BU).join(WA_BUA).fullOuterJoin(WA_BUV).fullOuterJoin(WA_BD_RATIO).fullOuterJoin(WA_BD).fullOuterJoin(WA_BDA).fullOuterJoin(WA_BDV).fullOuterJoin(WA_RATIO).map(lambda x:(tuple(flatten(x))))
schema3=StructType([  
        StructField('id',StringType(),True),
        StructField('WA_TIMES_RATIO',FloatType(),True),
        StructField('WA_TIMES',LongType(),True),
        StructField('WA_TIMESA',FloatType(),True),
        StructField('WA_TIMESV',FloatType(),True),
        StructField('WA_TIME_RATIO',FloatType(),True),
        StructField('WA_TIME',LongType(),True),
        StructField('WA_TIMEA',FloatType(),True),
        StructField('WA_TIMEV',FloatType(),True),
        StructField('WA_BU_RATIO',FloatType(),True),
        StructField('WA_BU',LongType(),True),
        StructField('WA_BUA',FloatType(),True),
        StructField('WA_BUV',FloatType(),True),
        StructField('WA_BD_RATIO',FloatType(),True),
        StructField('WA_BD',LongType(),True),      
        StructField('WA_BDA',FloatType(),True),
        StructField('WA_BDV',FloatType(),True),
        StructField('WA_RATIO',FloatType(),True)
        ]) 
TOTALDF=spark.createDataFrame(TOTAL,schema3)
TOTALDF.toPandas().to_csv('df\\TOTALDF.csv')



##选择含有‘联通’的记录方差
#网站
WA_LT_WEB_TIMESA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_WEB_TIMESV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0' and x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_WEB_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_WEB_TIMEA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_WEB_TIMEV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0' and x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_WEB_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_WEB_BUA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_WEB_BUV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0' and x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_WEB_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_WEB_BDA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_WEB_BDV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='0' and x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_WEB_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
(WA_LT_WEB_TIMESA.count(),WA_LT_WEB_TIMESV.count(),WA_LT_WEB_TIMEA.count(),WA_LT_WEB_TIMEV.count(),WA_LT_WEB_BUA.count(),WA_LT_WEB_BUV.count(),WA_LT_WEB_BDA.count(),WA_LT_WEB_BDV.count())

##RDD转为DF，指定模式
LTWEB=WA_LT_WEB_TIMESA.fullOuterJoin(WA_LT_WEB_TIMESV).fullOuterJoin(WA_LT_WEB_TIMEA).fullOuterJoin(WA_LT_WEB_TIMEV).fullOuterJoin(WA_LT_WEB_BUA).fullOuterJoin(WA_LT_WEB_BUV).fullOuterJoin(WA_LT_WEB_BDA).fullOuterJoin(WA_LT_WEB_BDV).map(lambda x:tuple(flatten(x)))
schema4=StructType([
        StructField('id',StringType(),True),
        StructField('WA_LT_WEB_TIMESA',FloatType(),True),
        StructField('WA_LT_WEB_TIMESV',FloatType(),True),
        StructField('WA_LT_WEB_TIMEA',FloatType(),True),
        StructField('WA_LT_WEB_TIMEV',FloatType(),True),
        StructField('WA_LT_WEB_BUA',FloatType(),True),
        StructField('WA_LT_WEB_BUV',FloatType(),True),
        StructField('WA_LT_WEB_BDA',FloatType(),True),
        StructField('WA_LT_WEB_BDV',FloatType(),True)
        ]) 
LTWEBDF=spark.createDataFrame(LTWEB,schema4) 
LTWEBDF.toPandas().to_csv('df\\LTWEBDF.csv')


#APP
WA_LT_APP_TIMESA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_TIMESV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_APP_TIMEA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_TIMEV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_APP_BUA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_BUV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_APP_BDA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_BDV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
(WA_LT_APP_TIMESA.count(),WA_LT_APP_TIMESV.count(),WA_LT_APP_TIMEA.count(),WA_LT_APP_TIMEV.count(),WA_LT_APP_BUA.count(),WA_LT_APP_BUV.count(),WA_LT_APP_BDA.count(),WA_LT_APP_BDV.count())

##RDD转为DF，指定模式
LTAPP=WA_LT_APP_TIMESA.fullOuterJoin(WA_LT_APP_TIMESV).fullOuterJoin(WA_LT_APP_TIMEA).fullOuterJoin(WA_LT_APP_TIMEV).fullOuterJoin(WA_LT_APP_BUA).fullOuterJoin(WA_LT_APP_BUV).fullOuterJoin(WA_LT_APP_BDA).fullOuterJoin(WA_LT_APP_BDV).map(lambda x:tuple(flatten(x)))
schema5=StructType([
        StructField('id',StringType(),True),
        StructField('WA_LT_APP_TIMESA',FloatType(),True),
        StructField('WA_LT_APP_TIMESV',FloatType(),True),
        StructField('WA_LT_APP_TIMEA',FloatType(),True),
        StructField('WA_LT_APP_TIMEV',FloatType(),True),
        StructField('WA_LT_APP_BUA',FloatType(),True),
        StructField('WA_LT_APP_BUV',FloatType(),True),
        StructField('WA_LT_APP_BDA',FloatType(),True),
        StructField('WA_LT_APP_BDV',FloatType(),True)
        ]) 
LTAPPDF=spark.createDataFrame(LTAPP,schema5) 
LTAPPDF.toPandas().to_csv('df\\LTAPPDF.csv')


#全部
WA_LT_TIMESA=wadata.filter(lambda x:'联通' in x[1] and x[2]!='NULL').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_TIMESV=wadata.filter(lambda x:'联通' in x[1] and x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_TIMEA=wadata.filter(lambda x:'联通' in x[1] and x[3]!='NULL').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_TIMEV=wadata.filter(lambda x:'联通' in x[1] and x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_BUA=wadata.filter(lambda x:'联通' in x[1] and x[4]!='NULL').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_BUV=wadata.filter(lambda x:'联通' in x[1] and x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_BDA=wadata.filter(lambda x:'联通' in x[1] and x[5]!='NULL').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_BDV=wadata.filter(lambda x:'联通' in x[1] and x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
(WA_LT_TIMESA.count(),WA_LT_TIMESV.count(),WA_LT_TIMEA.count(),WA_LT_TIMEV.count(),WA_LT_BUA.count(),WA_LT_BUV.count(),WA_LT_BDA.count(),WA_LT_BDV.count())

##RDD转为DF，指定模式
LTTOTAL=WA_LT_TIMESA.fullOuterJoin(WA_LT_TIMESV).fullOuterJoin(WA_LT_TIMEA).fullOuterJoin(WA_LT_TIMEV).fullOuterJoin(WA_LT_BUA).fullOuterJoin(WA_LT_BUV).fullOuterJoin(WA_LT_BDA).fullOuterJoin(WA_LT_BDV).map(lambda x:tuple(flatten(x)))
schema6=StructType([
        StructField('id',StringType(),True),
        StructField('WA_LT_TIMESA',FloatType(),True),
        StructField('WA_LT_TIMESV',FloatType(),True),
        StructField('WA_LT_TIMEA',FloatType(),True),
        StructField('WA_LT_TIMEV',FloatType(),True),
        StructField('WA_LT_BUA',FloatType(),True),
        StructField('WA_LT_BUV',FloatType(),True),
        StructField('WA_LT_BDA',FloatType(),True),
        StructField('WA_LT_BDV',FloatType(),True)
        ]) 
LTTOTALDF=spark.createDataFrame(LTTOTAL,schema6) 
LTTOTALDF.toPandas().to_csv('df\\LTTOTALDF.csv')



###周期波动
period_1=('45','44','43','42','41','40','39')
period_2=('38','37','36','35','34','33','32')
period_3=('31','30','29','28','27','26','25')
period_4=('24','23','22','21','20','19','18')
period_5=('17','16','15','14','13','12','11')
period_6=('10','09','08','07','06','05','04')
period=('45','44','43','42','41','40','39','38','37','36','35','34','33','32','31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04')

#次数
times_1=wadata.filter(lambda x:x[7] in period_1 and x[2]!='NULL').map(times).reduceByKey(funcmode2)
times_2=wadata.filter(lambda x:x[7] in period_2 and x[2]!='NULL').map(times).reduceByKey(funcmode2)
times_3=wadata.filter(lambda x:x[7] in period_3 and x[2]!='NULL').map(times).reduceByKey(funcmode2)
times_4=wadata.filter(lambda x:x[7] in period_4 and x[2]!='NULL').map(times).reduceByKey(funcmode2)
times_5=wadata.filter(lambda x:x[7] in period_5 and x[2]!='NULL').map(times).reduceByKey(funcmode2)
times_6=wadata.filter(lambda x:x[7] in period_6 and x[2]!='NULL').map(times).reduceByKey(funcmode2)
times_z=times_1.fullOuterJoin(times_2).fullOuterJoin(times_3).fullOuterJoin(times_4).fullOuterJoin(times_5).fullOuterJoin(times_6).map(lambda x:(x[0],tuple(flatten(x[1])))).mapValues(list).mapValues(lambda x:tuple(fillna(x)))
times_m=wadata.filter(lambda x:x[7] in period and x[2]!='NULL').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/6))
times_v=times_z.fullOuterJoin(times_m).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/6))

#时间
time_1=wadata.filter(lambda x:x[7] in period_1 and x[3]!='NULL').map(time).reduceByKey(funcmode2)
time_2=wadata.filter(lambda x:x[7] in period_2 and x[3]!='NULL').map(time).reduceByKey(funcmode2)
time_3=wadata.filter(lambda x:x[7] in period_3 and x[3]!='NULL').map(time).reduceByKey(funcmode2)
time_4=wadata.filter(lambda x:x[7] in period_4 and x[3]!='NULL').map(time).reduceByKey(funcmode2)
time_5=wadata.filter(lambda x:x[7] in period_5 and x[3]!='NULL').map(time).reduceByKey(funcmode2)
time_6=wadata.filter(lambda x:x[7] in period_6 and x[3]!='NULL').map(time).reduceByKey(funcmode2)
time_z=time_1.fullOuterJoin(time_2).fullOuterJoin(time_3).fullOuterJoin(time_4).fullOuterJoin(time_5).fullOuterJoin(time_6).map(lambda x:(x[0],tuple(flatten(x[1])))).mapValues(list).mapValues(lambda x:tuple(fillna(x)))
time_m=wadata.filter(lambda x:x[7] in period and x[3]!='NULL').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/6))
time_v=time_z.fullOuterJoin(time_m).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/6))

#上行流量
up_1=wadata.filter(lambda x:x[7] in period_1 and x[4]!='NULL').map(up).reduceByKey(funcmode2)
up_2=wadata.filter(lambda x:x[7] in period_2 and x[4]!='NULL').map(up).reduceByKey(funcmode2)
up_3=wadata.filter(lambda x:x[7] in period_3 and x[4]!='NULL').map(up).reduceByKey(funcmode2)
up_4=wadata.filter(lambda x:x[7] in period_4 and x[4]!='NULL').map(up).reduceByKey(funcmode2)
up_5=wadata.filter(lambda x:x[7] in period_5 and x[4]!='NULL').map(up).reduceByKey(funcmode2)
up_6=wadata.filter(lambda x:x[7] in period_6 and x[4]!='NULL').map(up).reduceByKey(funcmode2)
up_z=up_1.fullOuterJoin(up_2).fullOuterJoin(up_3).fullOuterJoin(up_4).fullOuterJoin(up_5).fullOuterJoin(up_6).map(lambda x:(x[0],tuple(flatten(x[1])))).mapValues(list).mapValues(lambda x:tuple(fillna(x)))
up_m=wadata.filter(lambda x:x[7] in period and x[4]!='NULL').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/6))
up_v=up_z.fullOuterJoin(up_m).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/6))

#下行流量
down_1=wadata.filter(lambda x:x[7] in period_1 and x[5]!='NULL').map(down).reduceByKey(funcmode2)
down_2=wadata.filter(lambda x:x[7] in period_2 and x[5]!='NULL').map(down).reduceByKey(funcmode2)
down_3=wadata.filter(lambda x:x[7] in period_3 and x[5]!='NULL').map(down).reduceByKey(funcmode2)
down_4=wadata.filter(lambda x:x[7] in period_4 and x[5]!='NULL').map(down).reduceByKey(funcmode2)
down_5=wadata.filter(lambda x:x[7] in period_5 and x[5]!='NULL').map(down).reduceByKey(funcmode2)
down_6=wadata.filter(lambda x:x[7] in period_6 and x[5]!='NULL').map(down).reduceByKey(funcmode2)
down_z=down_1.fullOuterJoin(down_2).fullOuterJoin(down_3).fullOuterJoin(down_4).fullOuterJoin(down_5).fullOuterJoin(down_6).map(lambda x:(x[0],tuple(flatten(x[1])))).mapValues(list).mapValues(lambda x:tuple(fillna(x)))
down_m=wadata.filter(lambda x:x[7] in period and x[5]!='NULL').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/6))
down_v=down_z.fullOuterJoin(down_m).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/6))

period_v=times_v.fullOuterJoin(time_v).fullOuterJoin(up_v).fullOuterJoin(down_v).map(lambda x:tuple(flatten(x)))
schema7=StructType([
        StructField('id',StringType(),True),
        StructField('times_v',FloatType(),True),
        StructField('time_v',FloatType(),True),
        StructField('up_v',FloatType(),True),
        StructField('down_v',FloatType(),True)
        ]) 
period_vdf=spark.createDataFrame(period_v,schema7) 
period_vdf.toPandas().to_csv('df\\period_vdf.csv')



#合并所有表格
import pandas as pd
WEBDF=pd.read_csv('df\\WEBDF.csv')
APPDF=pd.read_csv('df\\APPDF.csv')
TOTALDF=pd.read_csv('df\\TOTALDF.csv')
LTWEBDF=pd.read_csv('df\\LTWEBDF.csv')
LTAPPDF=pd.read_csv('df\\LTAPPDF.csv')
LTTOTALDF=pd.read_csv('df\\LTTOTALDF.csv')
period_vdf=pd.read_csv('df\\period_vdf.csv')
del WEBDF['Unnamed: 0']
del APPDF['Unnamed: 0']
del TOTALDF['Unnamed: 0']
del LTWEBDF['Unnamed: 0']
del LTAPPDF['Unnamed: 0']
del LTTOTALDF['Unnamed: 0']
del period_vdf['Unnamed: 0']
mergedata=pd.merge(WEBDF,APPDF,on='id',how='outer')
mergedata=pd.merge(mergedata,TOTALDF,on='id',how='outer')
mergedata=pd.merge(mergedata,LTWEBDF,on='id',how='outer')
mergedata=pd.merge(mergedata,LTAPPDF,on='id',how='outer')
mergedata=pd.merge(mergedata,LTTOTALDF,on='id',how='outer')
mergedata=pd.merge(mergedata,period_vdf,on='id',how='outer')
mergedata.shape
mergedata2=mergedata.fillna(0)
mergedata.to_csv('mergedata.csv')
mergedata2.to_csv('mergedata2.csv')


