# -*- coding: utf-8 -*-
"""
Created on Sun Jun  3 13:33:04 2018

@author: xiadanlin
"""

import pyspark
from pyspark import SparkContext
from operator import add

path_sms = r'E:\研究生课程\分布式统计计算\期末project\data\sms_train.txt'
sc = SparkContext()
sms = sc.textFile(path_sms)
smsdata = sms.map(lambda x:x.split('\t'))
smsdata.take(100)
funcmode1 = lambda x:(x[0],1)
funcmode2 = lambda x,y:x+y
funcmode3 = lambda x:(x[0],x[1]-1)
'''和时间无关的衍生变量'''
collect_data = sc.parallelize([('u'+str(i).zfill(4),1) for i in range(5000)])
SMS_RDD_CNT = smsdata.map(funcmode1).reduceByKey(funcmode2)
SMS_RDD_IN = smsdata.filter(lambda x:x[5]=='1').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_OUT = smsdata.filter(lambda x:x[5]=='0').map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_INOUTRATIO = SMS_RDD_IN.join(SMS_RDD_CNT).map(lambda x:(x[0],x[1][0]/x[1][1]))
SMS_RDD_HOWMANYPERSON = smsdata.filter(lambda x:x[3]=='11').map(lambda x:(x[0],x[1])).distinct().map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_HOWMANYBUSINESS = smsdata.filter(lambda x:x[3]!='11').map(lambda x:(x[0],x[1])).distinct().map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)

'''和时间有关的衍生变量'''
SMS_RDD_DAYTIME = smsdata.filter(lambda x:int(x[4][2:4])>=7 and int(x[4][2:4])<=12).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_AFTERNOON = smsdata.filter(lambda x:int(x[4][2:4])>=12 and int(x[4][2:4])<=18).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_NIGHT = smsdata.filter(lambda x:int(x[4][2:4])>=18 and int(x[4][2:4])<=24).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_BEFOREDAWN = smsdata.filter(lambda x:int(x[4][2:4])>=0 and int(x[4][2:4])<=7).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)

def generate_head():
    dianxin = ['133','149','153','173','177','180','181','189','199']
    liantong = ['130','131','132','145','155','156','166','171','175','176','185','186']
    yidong = ['134','135','136','137','138','139','147','150','151','152','157','158','159','178','182','183','184','187','188','198']
    return dianxin,liantong,yidong
dianxin,liantong,yidong = generate_head()
SMS_RDD_LIANTONG = smsdata.filter(lambda x:x[2] in liantong).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_DIANXIN = smsdata.filter(lambda x:x[2] in dianxin).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)
SMS_RDD_YIDONG = smsdata.filter(lambda x:x[2] in yidong).map(funcmode1).union(collect_data).reduceByKey(funcmode2).map(funcmode3)


