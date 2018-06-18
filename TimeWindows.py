# -*- coding: utf-8 -*-
"""
Created on Sat Jun 16 21:56:24 2018

@author: Administrator
"""

class TimeWindows(object):
    def __init__(self,rdd,StartTime,EndTime):
        self.datapiece = rdd.filter(lambda x:int(x[4][0:2])>StartTime and int(x[4][0:2])<EndTime)
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
                      StructField(paranames[0],Typeis(paranames[0]),True)]
        for para in paranames[1:]:
            structtype.append(StructField(para,Typeis(para),True))
            exec('self.join_ = self.join_.fullOuterJoin(self.{})'.format(para))
        self.join_ = self.join_.map(lambda x:tuple(flatten(x)))
        schema = StructType(structtype)
        DF = self.spark.createDataFrame(self.join_,schema)
        DF.toPandas().to_csv(self.basename+'.csv')