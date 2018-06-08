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
WA_WEB_RATIO=wadata.filter(lambda x:x[6]=='0').map(lambda x:(x[0],(int(x[4]),int(x[5])))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(int(x[1][0])/int(x[1][1])))
#WA_WEB_RATIO1=WA_WEB_BU.join(WA_WEB_BD).map(lambda x:(int(x[1][0])/int(x[1][1])))

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
WA_APP_RATIO=wadata.filter(lambda x:x[6]=='1').map(lambda x:(x[0],(int(x[4]),int(x[5])))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(int(x[1][0])/int(x[1][1])))

##总表信息
WA_TIMES_RATIO=WA_WEB_TIMES.join(WA_APP_TIMES).map(lambda x:(int(x[1][0])/int(x[1][1])))
WA_TIMES=wadata.filter(lambda x:x[2]!='NULL').map(times).reduceByKey(funcmode2)
WA_TIMESA=wadata.filter(lambda x:x[2]!='NULL').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_TIMESV=wadata.filter(lambda x:x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_TIME_RATIO=WA_WEB_TIME.join(WA_APP_TIME).map(lambda x:(int(x[1][0])/int(x[1][1])))
WA_TIME=wadata.filter(lambda x:x[3]!='NULL').map(time).reduceByKey(funcmode2)
WA_TIMEA=wadata.filter(lambda x:x[3]!='NULL').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_TIMEV=wadata.filter(lambda x:x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_BU_RATIO=WA_WEB_BU.join(WA_APP_BU).map(lambda x:(int(x[1][0])/int(x[1][1])))
WA_BU=wadata.filter(lambda x:x[4]!='NULL').map(up).reduceByKey(funcmode2)
WA_BUA=wadata.filter(lambda x:x[4]!='NULL').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_BUV=wadata.filter(lambda x:x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_BD_RATIO=WA_WEB_BD.join(WA_APP_BD).map(lambda x:(int(x[1][0])/int(x[1][1])))
WA_BD=wadata.filter(lambda x:x[5]!='NULL').map(down).reduceByKey(funcmode2)
WA_BDA=wadata.filter(lambda x:x[5]!='NULL').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_BDV=wadata.filter(lambda x:x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_RATIO=wadata.filter(lambda x:x[4]!='NULL' and x[5]!='NULL').map(lambda x:(x[0],(int(x[4]),int(x[5])))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(int(x[1][0])/int(x[1][1])))

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

#APP
WA_LT_APP_TIMESA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_TIMESV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_APP_TIMEA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_TIMEV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_APP_BUA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_BUV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_APP_BDA=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_APP_BDV=wadata.filter(lambda x:'联通' in x[1] and x[6]=='1' and x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_APP_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))

#全部
WA_LT_TIMESA=wadata.filter(lambda x:'联通' in x[1] and x[2]!='NULL').map(times).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_TIMESV=wadata.filter(lambda x:'联通' in x[1] and x[2]!='NULL').map(lambda x:((x[0],x[7]),x[2])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_TIMESA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_TIMEA=wadata.filter(lambda x:'联通' in x[1] and x[3]!='NULL').map(time).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_TIMEV=wadata.filter(lambda x:'联通' in x[1] and x[3]!='NULL').map(lambda x:((x[0],x[7]),x[3])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_TIMEA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_BUA=wadata.filter(lambda x:'联通' in x[1] and x[4]!='NULL').map(up).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_BUV=wadata.filter(lambda x:'联通' in x[1] and x[4]!='NULL').map(lambda x:((x[0],x[7]),x[4])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_BUA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))
WA_LT_BDA=wadata.filter(lambda x:'联通' in x[1] and x[5]!='NULL').map(down).reduceByKey(funcmode2).map(lambda x:(x[0],x[1]/45))
WA_LT_BDV=wadata.filter(lambda x:'联通' in x[1] and x[5]!='NULL').map(lambda x:((x[0],x[7]),x[5])).reduceByKey(lambda x,y:int(x)+int(y)).map(lambda x:(x[0][0],x[1])).groupByKey().mapValues(list).mapValues(lambda x:x+[0]*(45-len(x)) if len(x)!=45 else x).join(WA_LT_BDA).map(lambda x:(x[0],sum([(float(i)-float(x[1][1]))**2 for i in x[1][0]])/45))




#将APP和网站分开，然后取>200的类型
APP=wadata.filter(lambda x:x[6]=='1').map(funcmode3).reduceByKey(funcmode2).filter(lambda x: x[1]>200).collect()
import pandas as pd
APP=pd.DataFrame(APP)
APP.to_csv('APP.csv',encoding='gbk')






