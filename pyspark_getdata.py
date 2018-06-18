# -*- coding: utf-8 -*-
"""
Project:Pyspark final exam

@author: Fan
"""

import pyspark
from pyspark import SparkContext
from operator import add
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pandas as pd
from func import *

path_voice = r'E:\python.file\structure data\JDATA\voice_train.txt'
path_uid = r'E:\python.file\structure data\JDATA\uid_train.txt'
path_sms = r'E:\python.file\structure data\JDATA\sms_train.txt'
path_wa = r'E:\python.file\structure data\JDATA\wa_train.txt'


#create a spark shell to load .txt data
sc = SparkContext()
spark = SparkSession.builder.appName("dataFrameApply").getOrCreate()
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

def time_diff(forward,backward):
    day_diff = int(str(backward)[0:2])-int(str(forward)[0:2])
    hour_diff = int(str(backward[2:4]))-int(str(forward)[2:4])
    if hour_diff<0:hour_diff, day_diff = 24+hour_diff,day_diff-1
    minute_diff = int(str(backward[4:6]))-int(str(forward)[4:6])
    if minute_diff<0:minute_diff, hour_diff = 60+minute_diff,hour_diff-1
    second_diff = int(str(backward[6:]))-int(str(forward[6:]))
    if second_diff<0:second_diff, minute_diff = 60+second_diff,minute_diff-1
    freq_minute = day_diff*24*60 + hour_diff*60 + minute_diff + second_diff/60
    return freq_minute
        
def time_diff_iter(Iterator):
    Iterator.sort()
    for item_forward,item_backward in zip(Iterator[0:-1],Iterator[1:]):
        day_diff = int(str(item_backward)[0:2])-int(str(item_forward)[0:2])
        hour_diff = int(str(item_backward[2:4]))-int(str(item_forward)[2:4])
        if hour_diff<0:hour_diff, day_diff = 24+hour_diff,day_diff-1
        minute_diff = int(str(item_backward[4:6]))-int(str(item_forward)[4:6])
        if minute_diff<0:minute_diff, hour_diff = 60+minute_diff,hour_diff-1
        #second_diff = int(str(item_backward[6:]))-int(str(item_forward[6:]))
        #if second_diff<0:second_diff, minute_diff = 60+second_diff,minute_diff-1
        freq_hour = day_diff*24 + hour_diff + minute_diff/60 
        yield freq_hour

def times_per_day(Iterator):
    for day in range(1,46):
        day_stat = [abs(day-int(time[0:2])) for time in Iterator]
        times = len([item for item in day_stat if not item])
        yield (day,times)
        
def generate_head():
    dianxin = ['133','149','153','173','177','180','181','189','199']
    liantong = ['130','131','132','145','155','156','166','171','175','176','185','186']
    yidong = ['134','135','136','137','138','139','147','150','151','152','157','158','159','178','182','183','184','187','188','198']
    return dianxin,liantong,yidong
dianxin,liantong,yidong = generate_head()  
    
def mean(Iterator):
    sums = 0
    cnt = len(Iterator)
    for item in Iterator:
        sums += item
    return sums/(cnt+0.00001)
            
funcmode1 = lambda x:(x[0],1)
funcmode2 = lambda x,y:x+y
funcmode3 = lambda x:(x[0],float(x[1]-1))

#This function is used to add a sample in each Variable
#for sometimes the sample is not exist and ReduceByKey func cannot search the Key
#then the data with 0 times will be ignored. 
collect_data = sc.parallelize([('u'+str(i).zfill(4),1) for i in range(1,5000)])

CALL_RDD_RECORD = voicedata.map(funcmode1).distinct(). \
                  union(collect_data.subtract(voicedata.map(funcmode1).distinct()). \
                  map(lambda x:(x[0],0)))
call_type = {'LOCAL':1,'IN_PROVINCE':2,'BY_PROVINCE':3,'GAT':4,'INTERNATIONAL':5}
for type_ in call_type:
    exec("CALL_RDD_{} = voicedata.filter(lambda x:x[6]=='{}'). \
                        map(funcmode1).reduceByKey(funcmode2)". \
                        format(type_,str(call_type[type_])))
CALL_RDD_CNT = voicedata.map(funcmode1).reduceByKey(funcmode2)   
CALL_RDD_IN = voicedata.filter(lambda x:x[7]=='1'). \
              map(funcmode1).union(collect_data). \
              reduceByKey(funcmode2).map(funcmode3)
#CALL_RDD_OUT = voicedata.filter(lambda x:x[7]=='0').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_INOUTRATIO = CALL_RDD_IN.join(CALL_RDD_CNT). \
                      map(lambda x:(x[0],x[1][0]/(x[1][1]+0.0001)))
CALL_RDD_TIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                reduceByKey(funcmode2)
CALL_RDD_TIME_IN = voicedata.filter(lambda x:x[7]=='1'). \
                   map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                   union(collect_data).reduceByKey(funcmode2).map(funcmode3)
#CALL_RDD_TIME_OUT = voicedata.filter(lambda x:x[7]=='0').map(lambda x:(x[0],time_diff(x[4],x[5]))).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_TIME_INOUTRATIO = CALL_RDD_TIME_IN.join(CALL_RDD_TIME). \
                           map(lambda x:(x[0],x[1][0]/(x[1][1]+0.0001)))
CALL_RDD_TIME_PERSON = voicedata.filter(lambda x:x[3]=='11'). \
                       map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                       union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_TIME_BUSINESS = voicedata.filter(lambda x:x[3]!='11'). \
                         map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                         union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_TIME_PBRATIO = CALL_RDD_TIME_PERSON.join(CALL_RDD_TIME). \
                        map(lambda x:(x[0],x[1][0]/(x[1][1]+0.0001)))
CALL_RDD_PERSON = voicedata.filter(lambda x:x[3]=='11'). \
                  map(funcmode1).union(collect_data). \
                  reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_PERSON_IN = voicedata.filter(lambda x:x[3]=='11' and x[7]=='1'). \
                     map(funcmode1).union(collect_data). \
                     reduceByKey(funcmode2).map(funcmode3)
#CALL_RDD_PERSON_OUT = voicedata.filter(lambda x:x[3]=='11' and x[7]=='0').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_PERSON_RATIO = CALL_RDD_PERSON_IN.join(CALL_RDD_PERSON). \
                        map(lambda x:(x[0],x[1][0]/(x[1][1]+0.0001)))
CALL_RDD_BUSINESS = voicedata.filter(lambda x:x[3]!='11'). \
                    map(funcmode1).union(collect_data). \
                    reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_BUSINESS_IN = voicedata.filter(lambda x:x[3]!='11' and x[7]=='1'). \
                       map(funcmode1).union(collect_data). \
                       reduceByKey(funcmode2).map(funcmode3)
#CALL_RDD_BUSINESS_OUT = voicedata.filter(lambda x:x[3]!='11' and x[7]=='0').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_BUSINESS_RATIO = CALL_RDD_BUSINESS_IN.join(CALL_RDD_BUSINESS). \
                          map(lambda x:(x[0],x[1][0]/(x[1][1]+0.0001)))  
CALL_RDD_PBRATIO = CALL_RDD_PERSON.join(CALL_RDD_CNT). \
                   map(lambda x:(x[0],x[1][0]/(x[1][1]+0.0001)))
CALL_RDD_HOWMANYPERSON = voicedata.filter(lambda x:x[3]=='11' and x[2][1]=='1'). \
                         map(lambda x:(x[0],x[1])).distinct(). \
                         map(funcmode1).union(collect_data). \
                         reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_HOWMANYBUSINESS = voicedata.filter(lambda x:x[3]!='11' or x[2][1]!='1'). \
                           map(lambda x:(x[0],x[1])).distinct(). \
                           map(funcmode1).union(collect_data). \
                           reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_AVGTIME = voicedata.map(lambda x:(x[0],(time_diff(x[4],x[5]),1))). \
                   reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])). \
                   map(lambda x:(x[0],x[1][0]/(x[1][1]+0.0001)))
CALL_RDD_DAYTIME = voicedata.filter(lambda x:int(x[4][2:4])>=7 and int(x[4][2:4])<=12). \
                   map(funcmode1).union(collect_data). \
                   reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_AFTERNOON = voicedata.filter(lambda x:int(x[4][2:4])>=12 and int(x[4][2:4])<=18). \
                     map(funcmode1).union(collect_data). \
                     reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_NIGHT = voicedata.filter(lambda x:int(x[4][2:4])>=18 and int(x[4][2:4])<=24). \
                 map(funcmode1).union(collect_data). \
                 reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_BEFOREDAWN = voicedata.filter(lambda x:int(x[4][2:4])>=0 and int(x[4][2:4])<=7). \
                      map(funcmode1).union(collect_data). \
                      reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_MAXTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                   reduceByKey(lambda x,y:max(x,y))
CALL_RDD_MINTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                   reduceByKey(lambda x,y:min(x,y))
CALL_RDD_TIMEVAR = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                   join(CALL_RDD_AVGTIME).join(CALL_RDD_CNT). \
                   map(lambda x:(x[0],tuple(flatten(x[1])))). \
                   map(lambda x:(x[0],(x[1][0]-x[1][1])**2/(x[1][2]+0.0001))).reduceByKey(funcmode2) 
#Some people doesn't have voice record ,fill the data with 45x24
fill_freq = sc.parallelize([('u'+str(i).zfill(4),24*45) for i in range(1,5000)])
call_rdd_timediff = voicedata.map(lambda x:(x[0],x[4])).groupByKey(). \
                    mapValues(list).filter(lambda x:len(x[1])!=1)
CALL_RDD_MEANFREQ = call_rdd_timediff.map(lambda x:(x[0],mean(list(time_diff_iter(x[1]))))). \
                    union(fill_freq).reduceByKey(lambda x,y:min(x,y)).map(lambda x:(x[0],float(x[1])))
CALL_RDD_MINFREQ = call_rdd_timediff.map(lambda x:(x[0],min(list(time_diff_iter(x[1]))))). \
                   union(fill_freq).reduceByKey(lambda x,y:min(x,y)).map(lambda x:(x[0],float(x[1])))
CALL_RDD_MAXFREQ = call_rdd_timediff.map(lambda x:(x[0],max(list(time_diff_iter(x[1]))))). \
                   union(fill_freq).reduceByKey(lambda x,y:min(x,y)).map(lambda x:(x[0],float(x[1])))
###
CALL_RDD_VARFREQ =  call_rdd_timediff.mapValues(lambda x:list(time_diff_iter(x))). \
                    union(collect_data.subtract(call_rdd_timediff.map(lambda x:(x[0],1)).distinct()). \
                    map(lambda x:(x[0],[24*45]))). \
                    join(CALL_RDD_MEANFREQ). \
                    mapValues(lambda x:mean([(i-x[1])**2 for i in x[0]]))                    
###
CALL_RDD_FREQONEDAY = CALL_RDD_MEANFREQ.map(lambda x:(x[0],24/(x[1]+0.0001)))
CALL_RDD_SHORTTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                     filter(lambda x:x[1]<=1).map(funcmode1). \
                     union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_LONGTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                    filter(lambda x:x[1]>5 and x[1]<20).map(funcmode1). \
                    union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_LLONGTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                     filter(lambda x:x[1]>=20).map(funcmode1). \
                     union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_LIANTONG = voicedata.filter(lambda x:x[2] in liantong).map(funcmode1). \
                    union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_YIDONG = voicedata.filter(lambda x:x[2] in yidong).map(funcmode1). \
                  union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_DIANXIN = voicedata.filter(lambda x:x[2] in dianxin).map(funcmode1). \
                   union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_KEFU = voicedata.filter(lambda x:x[2]=='1').map(funcmode1). \
                union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_DAYWITHCALL = voicedata.map(lambda x:(x[0],x[4][0:2])).distinct(). \
                       groupByKey().mapValues(list).mapValues(lambda x:len(x))
CALL_RDD_DAYWITHCALLIN = voicedata.filter(lambda x:x[7]=='1'). \
                         map(lambda x:(x[0],x[4][0:2])).distinct(). \
                         groupByKey().mapValues(list).mapValues(lambda x:len(x))
CALL_RDD_DAYWITHCALLOUT = voicedata.filter(lambda x:x[7]=='0'). \
                          map(lambda x:(x[0],x[4][0:2])).distinct(). \
                          groupByKey().mapValues(list).mapValues(lambda x:len(x))

CALL_RDD_JOIN = CALL_RDD_RECORD. \
                fullOuterJoin(CALL_RDD_CNT). \
                fullOuterJoin(CALL_RDD_IN). \
                fullOuterJoin(CALL_RDD_LOCAL). \
                fullOuterJoin(CALL_RDD_IN_PROVINCE). \
                fullOuterJoin(CALL_RDD_BY_PROVINCE). \
                fullOuterJoin(CALL_RDD_GAT). \
                fullOuterJoin(CALL_RDD_INTERNATIONAL). \
                fullOuterJoin(CALL_RDD_TIME). \
                fullOuterJoin(CALL_RDD_TIME_IN). \
                fullOuterJoin(CALL_RDD_TIME_INOUTRATIO). \
                fullOuterJoin(CALL_RDD_TIME_PERSON). \
                fullOuterJoin(CALL_RDD_TIME_BUSINESS). \
                fullOuterJoin(CALL_RDD_TIME_PBRATIO). \
                fullOuterJoin(CALL_RDD_PERSON). \
                fullOuterJoin(CALL_RDD_PERSON_IN). \
                fullOuterJoin(CALL_RDD_PERSON_RATIO). \
                fullOuterJoin(CALL_RDD_BUSINESS). \
                fullOuterJoin(CALL_RDD_BUSINESS_IN). \
                fullOuterJoin(CALL_RDD_BUSINESS_RATIO). \
                fullOuterJoin(CALL_RDD_PBRATIO). \
                fullOuterJoin(CALL_RDD_HOWMANYPERSON). \
                fullOuterJoin(CALL_RDD_HOWMANYBUSINESS). \
                fullOuterJoin(CALL_RDD_AVGTIME). \
                fullOuterJoin(CALL_RDD_DAYTIME). \
                fullOuterJoin(CALL_RDD_AFTERNOON). \
                fullOuterJoin(CALL_RDD_NIGHT). \
                fullOuterJoin(CALL_RDD_BEFOREDAWN). \
                fullOuterJoin(CALL_RDD_MAXTIME). \
                fullOuterJoin(CALL_RDD_MINTIME). \
                fullOuterJoin(CALL_RDD_TIMEVAR). \
                fullOuterJoin(CALL_RDD_MEANFREQ). \
                fullOuterJoin(CALL_RDD_MINFREQ). \
                fullOuterJoin(CALL_RDD_MAXFREQ). \
                fullOuterJoin(CALL_RDD_VARFREQ). \
                fullOuterJoin(CALL_RDD_FREQONEDAY). \
                fullOuterJoin(CALL_RDD_SHORTTIME). \
                fullOuterJoin(CALL_RDD_LONGTIME). \
                fullOuterJoin(CALL_RDD_LLONGTIME). \
                fullOuterJoin(CALL_RDD_LIANTONG). \
                fullOuterJoin(CALL_RDD_YIDONG). \
                fullOuterJoin(CALL_RDD_DIANXIN). \
                fullOuterJoin(CALL_RDD_KEFU). \
                fullOuterJoin(CALL_RDD_DAYWITHCALL). \
                fullOuterJoin(CALL_RDD_DAYWITHCALLIN). \
                fullOuterJoin(CALL_RDD_DAYWITHCALLOUT). \
                map(lambda x:tuple(flatten(x)))

schema = StructType([StructField('id',StringType(),True),
                     StructField('CALL_RDD_RECORD',LongType(),True),
                     StructField('CALL_RDD_CNT',LongType(),True),
                     StructField('CALL_RDD_CNT_IN',FloatType(),True),
                     StructField('CALL_RDD_LOCAL',LongType(),True),
                     StructField('CALL_RDD_IN_PROVINCE',LongType(),True),
                     StructField('CALL_RDD_BY_PROVINCE',LongType(),True),
                     StructField('CALL_RDD_GAT',LongType(),True),
                     StructField('CALL_RDD_INTERNATIONAL',LongType(),True),
                     StructField('CALL_RDD_TIME',FloatType(),True),
                     StructField('CALL_RDD_TIME_IN',FloatType(),True),
                     StructField('CALL_RDD_TIME_INOUTRATIO',FloatType(),True),
                     StructField('CALL_RDD_TIME_PERSON',FloatType(),True),
                     StructField('CALL_RDD_TIME_BUSINESS',FloatType(),True),
                     StructField('CALL_RDD_TIME_PBRATIO',FloatType(),True),
                     StructField('CALL_RDD_PERSON',FloatType(),True),
                     StructField('CALL_RDD_PERSON_IN',FloatType(),True),
                     StructField('CALL_RDD_PERSON_RATIO',FloatType(),True),
                     StructField('CALL_RDD_BUSINESS',FloatType(),True),
                     StructField('CALL_RDD_BUSINESS_IN',FloatType(),True),
                     StructField('CALL_RDD_BUSINESS_RATIO',FloatType(),True),
                     StructField('CALL_RDD_PBRATIO',FloatType(),True),
                     StructField('CALL_RDD_HOWMANYPERSON',FloatType(),True),
                     StructField('CALL_RDD_HOWMANYBUSINESS',FloatType(),True),
                     StructField('CALL_RDD_AVGTIME',FloatType(),True),
                     StructField('CALL_RDD_DAYTIME',FloatType(),True),
                     StructField('CALL_RDD_AFTERNOON',FloatType(),True),
                     StructField('CALL_RDD_NIGHT',FloatType(),True),
                     StructField('CALL_RDD_BEFOREDAWN',FloatType(),True),
                     StructField('CALL_RDD_MAXTIME',FloatType(),True),
                     StructField('CALL_RDD_MINTIME',FloatType(),True),
                     StructField('CALL_RDD_TIMEVAR',FloatType(),True),
                     StructField('CALL_RDD_MEANFREQ',FloatType(),True),
                     StructField('CALL_RDD_MINFREQ',FloatType(),True),
                     StructField('CALL_RDD_MAXFREQ',FloatType(),True),
                     StructField('CALL_RDD_VARFREQ',FloatType(),True),
                     StructField('CALL_RDD_FREQONEDAY',FloatType(),True),
                     StructField('CALL_RDD_SHORTTIME',FloatType(),True),
                     StructField('CALL_RDD_LONGTIME',FloatType(),True),
                     StructField('CALL_RDD_LLONGTIME',FloatType(),True),
                     StructField('CALL_RDD_LIANTONG',FloatType(),True),
                     StructField('CALL_RDD_YIDONG',FloatType(),True),
                     StructField('CALL_RDD_DIANXIN',FloatType(),True),
                     StructField('CALL_RDD_KEFU',FloatType(),True),
                     StructField('CALL_RDD_DAYWITHCALL',LongType(),True),
                     StructField('CALL_RDD_DAYWITHCALLIN',LongType(),True),
                     StructField('CALL_RDD_DAYWITHCALLOUT',LongType(),True)])

    
DF = spark.createDataFrame(CALL_RDD_JOIN,schema)
DF.toPandas().to_csv('CALL_RDD_FULL'+'.csv')
call_per_person = call_rdd_timediff.mapValues(lambda x:list(times_per_day(x)))   

#Time windows method
class TimeWindows(object):
    def __init__(self,rdd,StartTime,EndTime):
        self.datapiece = rdd.filter(lambda x:int(x[4][0:2])>=StartTime and int(x[4][0:2])<=EndTime)
        self.basename = 'CALL_RDD_DAY{}TODAY{}_'.format(StartTime,EndTime)
        self.collect_data = sc.parallelize([('u'+str(i).zfill(4),1) for i in range(1,5000)])
        self.spark = SparkSession.builder.appName("dataFrameApply").getOrCreate()
    
    def generate(self):
        def time_diff(forward,backward):
            day_diff = int(str(backward)[0:2])-int(str(forward)[0:2])
            hour_diff = int(str(backward[2:4]))-int(str(forward)[2:4])
            if hour_diff<0:hour_diff, day_diff = 24+hour_diff,day_diff-1
            minute_diff = int(str(backward[4:6]))-int(str(forward)[4:6])
            if minute_diff<0:minute_diff, hour_diff = 60+minute_diff,hour_diff-1
            second_diff = int(str(backward[6:]))-int(str(forward[6:]))
            if second_diff<0:second_diff, minute_diff = 60+second_diff,minute_diff-1
            freq_minute = day_diff*24*60 + hour_diff*60 + minute_diff + second_diff/60
            return freq_minute
    
        def flatten(Iterator):
            for item in Iterator:
                if isinstance(item,(list,tuple)):
                    for sub_item in flatten(item):
                        yield sub_item
                else:yield item
        funcmode1 = lambda x:(x[0],1)
        funcmode2 = lambda x,y:x+y
        funcmode3 = lambda x:(x[0],float(x[1]-1))
        self.RECORD = self.datapiece.map(funcmode1).distinct(). \
                         union(self.collect_data.subtract(self.datapiece.map(funcmode1).distinct()). \
                         map(lambda x:(x[0],0)))
        self.CNT = self.datapiece.map(funcmode1).reduceByKey(funcmode2)
        self.IN = self.datapiece.filter(lambda x:x[7]=='1'). \
              map(funcmode1).union(self.collect_data). \
              reduceByKey(funcmode2).map(funcmode3)
        self.INOUTRATIO = self.IN.join(self.CNT). \
                           map(lambda x:(x[0],x[1][0]/x[1][1]))
        self.TIME = self.datapiece.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                reduceByKey(funcmode2)
        self.TIME_IN = self.datapiece.filter(lambda x:x[7]=='1'). \
                   map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                   union(self.collect_data).reduceByKey(funcmode2).map(funcmode3)
        self.TIME_INOUTRATIO = self.TIME_IN.join(self.TIME). \
                           map(lambda x:(x[0],x[1][0]/x[1][1]))
        self.TIME_AVG = self.datapiece.map(lambda x:(x[0],(time_diff(x[4],x[5]),1))). \
                   reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])). \
                   map(lambda x:(x[0],x[1][0]/x[1][1]))
        self.TIME_VAR = self.datapiece.map(lambda x:(x[0],time_diff(x[4],x[5]))). \
                   join(self.TIME_AVG).join(self.CNT). \
                   map(lambda x:(x[0],tuple(flatten(x[1])))). \
                   map(lambda x:(x[0],(x[1][0]-x[1][1])**2/x[1][2])).reduceByKey(funcmode2) 
                   
    def generateMore(self):
        pass       
 
    def join(self):
        def flatten(Iterator):
            for item in Iterator:
                if isinstance(item,(list,tuple)):
                    for sub_item in flatten(item):
                        yield sub_item
                else:yield item
                
        def Typeis(para):
            exec('self.type_ = type(self.{}.take(1)[0][1])'.format(para))
            if self.type_ == str:
                return StringType()
            elif self.type_ == int:
                return LongType()
            elif self.type_ == float:
                return FloatType()
            
        paranames = [i for i in dir(self) if i[0]!='_' and i.isupper()]
        exec('self.join_ = self.{}'.format(paranames[0]))
        structtype = [StructField('id',StringType(),True),
                      StructField(self.basename+paranames[0],Typeis(paranames[0]),True)]
        for para in paranames[1:]:
            structtype.append(StructField(self.basename+para,Typeis(para),True))
            exec('self.join_ = self.join_.fullOuterJoin(self.{})'.format(para))
        self.join_ = self.join_.map(lambda x:tuple(flatten(x)))
        schema = StructType(structtype)
        DF = self.spark.createDataFrame(self.join_,schema)
        DF.toPandas().to_csv(self.basename+'.csv')










