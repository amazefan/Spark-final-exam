# -*- coding: utf-8 -*-
"""
Project:Pyspark final exam

@author: Fan
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
funcmode3 = lambda x:(x[0],x[1]-1)

#This function is used to add a sample in each Variable
#for sometimes the sample is not exist and ReduceByKey func cannot search the Key
#then the data with 0 times will be ignored. 
collect_data = sc.parallelize([('u'+str(i).zfill(4),1) for i in range(5000)])

call_type = {'LOCAL':1,'IN_PROVINCE':2,'BY_PROVINCE':3,'GAT':4,'INTERNATIONAL':5}
for type_ in call_type:
    exec("CALL_RDD_{} = voicedata.filter(lambda x:x[6]=='{}').map(funcmode1).reduceByKey(funcmode2)".format(type_,str(call_type[type_])))
CALL_RDD_CNT = voicedata.map(funcmode1).reduceByKey(funcmode2)   
CALL_RDD_IN = voicedata.filter(lambda x:x[7]=='1').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_OUT = voicedata.filter(lambda x:x[7]=='0').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_INOUTRATIO = CALL_RDD_IN.join(CALL_RDD_CNT).map(lambda x:(x[0],x[1][0]/x[1][1]))
CALL_RDD_TIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))).reduceByKey(funcmode2)
CALL_RDD_TIME_IN = voicedata.filter(lambda x:x[7]=='1').map(lambda x:(x[0],time_diff(x[4],x[5]))).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_TIME_OUT = voicedata.filter(lambda x:x[7]=='0').map(lambda x:(x[0],time_diff(x[4],x[5]))).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_TIME_INOUTRATIO = CALL_RDD_TIME_IN.join(CALL_RDD_TIME).map(lambda x:(x[0],x[1][0]/x[1][1]))
CALL_RDD_TIME_PERSON = voicedata.filter(lambda x:x[3]=='11').map(lambda x:(x[0],time_diff(x[4],x[5]))).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_TIME_BUSINESS = voicedata.filter(lambda x:x[3]!='11').map(lambda x:(x[0],time_diff(x[4],x[5]))).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_TIME_PBRATIO = CALL_RDD_TIME_PERSON.join(CALL_RDD_TIME).map(lambda x:(x[0],x[1][0]/x[1][1]))
CALL_RDD_PERSON = voicedata.filter(lambda x:x[3]=='11').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_PERSON_IN = voicedata.filter(lambda x:x[3]=='11' and x[7]=='1').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_PERSON_OUT = voicedata.filter(lambda x:x[3]=='11' and x[7]=='0').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_PERSON_RATIO = CALL_RDD_PERSON_IN.join(CALL_RDD_PERSON).map(lambda x:(x[0],x[1][0]/x[1][1]))
CALL_RDD_BUSINESS = voicedata.filter(lambda x:x[3]!='11').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_BUSINESS_IN = voicedata.filter(lambda x:x[3]!='11' and x[7]=='1').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_BUSINESS_OUT = voicedata.filter(lambda x:x[3]!='11' and x[7]=='0').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_BUSINESS_RATIO = CALL_RDD_BUSINESS_IN.join(CALL_RDD_BUSINESS).map(lambda x:(x[0],x[1][0]/x[1][1]))  
CALL_RDD_PBRATIO = CALL_RDD_PERSON.join(CALL_RDD_CNT).map(lambda x:(x[0],x[1][0]/x[1][1]))
CALL_RDD_HOWMANYPERSON = voicedata.filter(lambda x:x[3]=='11').map(lambda x:(x[0],x[1])).distinct().map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_HOWMANYBUSINESS = voicedata.filter(lambda x:x[3]!='11').map(lambda x:(x[0],x[1])).distinct().map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_AVGTIME = voicedata.map(lambda x:(x[0],(time_diff(x[4],x[5]),1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],x[1][0]/x[1][1]))
CALL_RDD_DAYTIME = voicedata.filter(lambda x:int(x[4][2:4])>=7 and int(x[4][2:4])<=12).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_AFTERNOON = voicedata.filter(lambda x:int(x[4][2:4])>=12 and int(x[4][2:4])<=18).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_NIGHT = voicedata.filter(lambda x:int(x[4][2:4])>=18 and int(x[4][2:4])<=24).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_BEFOREDAWN = voicedata.filter(lambda x:int(x[4][2:4])>=0 and int(x[4][2:4])<=7).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_MAXTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))).reduceByKey(lambda x,y:max(x,y))
CALL_RDD_MINTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))).reduceByKey(lambda x,y:min(x,y))
CALL_RDD_TIMEVAR = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))).join(CALL_RDD_AVGTIME).join(CALL_RDD_CNT).map(lambda x:(x[0],tuple(flatten(x[1])))).map(lambda x:(x[0],(x[1][0]-x[1][1])**2/x[1][2])).reduceByKey(funcmode2) 

#Some people doesn't have voice record ,fill the data with 45x24
fill_freq = sc.parallelize([('u'+str(i).zfill(4),24*45) for i in range(5000)])
CALL_RDD_MEANFREQ = voicedata.map(lambda x:(x[0],x[4])).groupByKey().mapValues(list).filter(lambda x:len(x[1])!=1).map(lambda x:(x[0],mean(list(time_diff_iter(x[1]))))).union(fill_freq).reduceByKey(lambda x,y:min(x,y))
###
CALL_RDD_FREQONEDAY = CALL_RDD_MEANFREQ.map(lambda x:(x[0],24/x[1]))
CALL_RDD_SHORTTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))).filter(lambda x:x[1]<=1).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_LONGTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))).filter(lambda x:x[1]>5 and x[1]<20).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_LLONGTIME = voicedata.map(lambda x:(x[0],time_diff(x[4],x[5]))).filter(lambda x:x[1]>=20).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_LIANTONG = voicedata.filter(lambda x:x[2] in liantong).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_YIDONG = voicedata.filter(lambda x:x[2] in yidong).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_DIANXIN = voicedata.filter(lambda x:x[2] in dianxin).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
CALL_RDD_KEFU = voicedata.filter(lambda x:x[2]=='1').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)

person = voicedata.map(lambda x:(x[0],x[4][0:2])).groupByKey().mapValues(list)








