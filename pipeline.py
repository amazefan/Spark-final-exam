# -*- coding: utf-8 -*-
"""

@author: Fan
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
from pyspark.ml.classification import LinearSVC

from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os
os.chdir(r'E:\python.file\git-Repository\Spark-final-exam\data/')

from pyspark.sql.functions import udf
from pyspark.sql.functions import col
import pyspark.sql.types
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
    print('验证集GBDT模型AUC为: '+str(auc))
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
    print('验证集RANDOM FOREST模型AUC为: '+str(auc))
    return auc

def SVMpipeline(train,validation):
    features= train.columns[1:-1]
    featureAssembler =VectorAssembler(inputCols=features, outputCol="Features")
    SVM= LinearSVC(labelCol='lable',featuresCol='Features')
    SVM_pipeline=Pipeline(stages=[featureAssembler,SVM])
    SVM_model =SVM_pipeline.fit(train)
    print('评估模型中...')
    predicted=SVM_model.transform(validation)
    evaluator=BinaryClassificationEvaluator(rawPredictionCol='rawPrediction',labelCol='lable',metricName='areaUnderROC')
    auc=evaluator.evaluate(predicted)
    print('验证集SVM模型AUC为: '+str(auc))
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

#stacking
import pyspark.sql.functions as fn
vectorToColumn = fn.udf(lambda vec: vec[1].item(), DoubleType())


class Stacking(object):
    def __init__(self,models,train,validation):
        self.models = models
        self.train = train
        self.validation = validation
    
    @staticmethod    
    def basic(train,validation,model):
        colname = str(model)
        features= train.columns[1:-1]
        featureAssembler =VectorAssembler(inputCols=features, outputCol="Features")
        pipeline=Pipeline(stages=[featureAssembler,model])
        model = pipeline.fit(train)
        print('评估模型中...')
        predicted1=model.transform(train)
        auc_calculator = model.transform(validation)
        evaluator=BinaryClassificationEvaluator(rawPredictionCol='probability',labelCol='lable',metricName='areaUnderROC')
        auc=evaluator.evaluate(auc_calculator)
        print('验证集模型AUC为:'+str(auc))
        predicted1 = predicted1.withColumn(colname,vectorToColumn(predicted1.probability))
        predicted1 = predicted1.drop('Features','probability','prediction','rawPrediction')
        predicted2 = auc_calculator.withColumn(colname,vectorToColumn(auc_calculator.probability))
        predicted2 = predicted2.drop('Features','probability','prediction','rawPrediction') 
        return predicted1.select('id',colname),predicted2.select('id',colname)

    @staticmethod
    def stacking(model_probs):
        basic_DF = model_probs[0]
        for DF in model_probs[1:]:
            basic_DF = basic_DF.join(DF,basic_DF.id == DF.id).drop(DF.id)
        return basic_DF
    
    @staticmethod
    def logistic_pipeline(train,validation):
        features= train.columns[2:]
        featureAssembler =VectorAssembler(inputCols=features, outputCol="Features")
        LR= LogisticRegression(labelCol='lable',featuresCol='Features')
        LR_pipeline=Pipeline(stages=[featureAssembler,LR])
        LR_model = LR_pipeline.fit(train)
        print('评估模型中...')
        predicted=LR_model.transform(validation)
        evaluator=BinaryClassificationEvaluator(rawPredictionCol='probability',labelCol='lable',metricName='areaUnderROC')
        auc=evaluator.evaluate(predicted)
        print('验证集Stacking模型AUC为:'+str(auc))
        return auc,predicted

    def trainStacking(self):
        train_probs = [self.train.select('id','lable')]
        validation_probs = [self.validation.select('id','lable')]
        for model in self.models:
            probs_train ,probs_validation= self.basic(train = self.train ,validation = self.validation ,model = model)
            train_probs.append(probs_train)
            validation_probs.append(probs_validation)
        self.DF_train = self.stacking(train_probs)
        self.DF_validation = self.stacking(validation_probs)
        self.auc, self.predicted = self.logistic_pipeline(self.DF_train,self.DF_validation)
        return self.auc


train,validation = getdata('all1.csv')
model1 = RandomForestClassifier(labelCol='lable',featuresCol='Features',maxDepth=10,numTrees=500)
model2 = GBTClassifier(labelCol='lable',featuresCol='Features',maxIter=6)
model3 = RandomForestClassifier(labelCol='lable',featuresCol='Features',maxDepth=20,numTrees=250)
model4 = RandomForestClassifier(labelCol='lable',featuresCol='Features',maxDepth=10,numTrees=100)
models = [model1,model2,model3,model4]
ensemble = Stacking(models,train,validation)
ensemble.trainStacking()





















