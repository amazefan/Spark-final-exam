# -*- coding: utf-8 -*-
"""
Created on Wed Jun 20 14:32:39 2018

@author: Administrator
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession  
from pyspark.sql.types import StringType,StructField,StructType,DoubleType
#from pyspark.mllib.regression import LabeledPoint
#from pyspark.sql.functions import col
from pyspark.ml import Pipeline
import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer, VectorIndexer,Normalizer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier  
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import LogisticRegression

from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os
os.chdir(r'E:\python.file\git-Repository\Spark-final-exam\data/')

from pyspark.sql.functions import udf
from pyspark.sql.functions import col
import pyspark.sql.types


data = spark.read.format('csv')\
        .option('header','true')\
        .option('delimiter',',')\
        .load('all1.csv')
df_train = data.select(['id']+[col(column).cast('double').alias(column) for column in data.columns[1:] ])

train,validation = df_train.randomSplit([0.8,0.2])
features=train.columns[1:-1]
featureAssembler =VectorAssembler(inputCols=features, outputCol="Features")
gbdt=GBTClassifier(labelCol='lable',featuresCol='Features',maxIter=6)

spark = SparkSession.builder.appName("dataFrameApply").getOrCreate()
def getdata(datapath):
    data = spark.read.format('csv')\
        .option('header','true')\
        .option('delimiter',',')\
        .load(datapath)
    data_new = data.select(['id']+[col(column).cast('double').alias(column) for column in data.columns[1:] ])
    train,validation = data_new.randomSplit([0.8,0.2])
    return train,validation

def GBDTpipeline(train,validation):
    features= train.columns[1:-1]
    featureAssembler =VectorAssembler(inputCols=features, outputCol="Features")
    gbdt=GBTClassifier(labelCol='lable',featuresCol='Features',maxIter=6)
    gbdt_pipline=Pipeline(stages=[featureAssembler,gbdt])
    gbdt_model=gbdt_pipline.fit(train)
    print('评估模型中...')
    predicted=gbdt_model.transform(validation)
    evaluator=BinaryClassificationEvaluator(rawPredictionCol='rawPrediction',labelCol='lable',metricName='areaUnderROC')
    auc=evaluator.evaluate(predicted)
    print('验证集GBDT模型AUC为:'+str(auc))
    return auc

def RFpipeline(train,validation):
    features= train.columns[1:-1]
    featureAssembler =VectorAssembler(inputCols=features, outputCol="Features")
    rf= RandomForestClassifier(labelCol='lable',featuresCol='Features',maxDepth=10,numTrees=500)
    rf_pipeline=Pipeline(stages=[featureAssembler,rf])
    rf_model = rf_pipeline.fit(train)
    print('评估模型中...')
    predicted=rf_model.transform(validation)
    evaluator=BinaryClassificationEvaluator(rawPredictionCol='rawPrediction',labelCol='lable',metricName='areaUnderROC')
    auc=evaluator.evaluate(predicted)
    print('验证集RANDOM FOREST模型AUC为:'+str(auc))
    return auc

def logistic_pipeline(train,validation):
    features= train.columns[1:-1]
    featureAssembler =VectorAssembler(inputCols=features, outputCol="Features")
    LR= LogisticRegression(labelCol='lable',featuresCol='Features')
    LR_pipeline=Pipeline(stages=[featureAssembler,LR])
    LR_model = LR_pipeline.fit(train)
    print('评估模型中...')
    predicted=LR_model.transform(validation)
    evaluator=BinaryClassificationEvaluator(rawPredictionCol='rawPrediction',labelCol='lable',metricName='areaUnderROC')
    auc=evaluator.evaluate(predicted)
    print('验证集logistic模型AUC为:'+str(auc))
    return auc

def standard(data):
    for i in data.columns[1:-1]:
        normalize = Normalizer(inputCol = i, outputCol= i+'_std')
        data = normalize.transform(data)
    return data
    
    
    
    