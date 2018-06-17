# -*- coding: utf-8 -*-
"""
Created on Sun Jun 17 11:11:56 2018

@author: 98486
"""

from pyspark import SparkContext
from pyspark.sql import SQLContext
from time import time
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.evaluation import MulticlassMetrics
import os
os.chdir(r'C:\Users\98486\Desktop\风险用户识别\data')
sc=SparkContext()
sqlContext=SQLContext(sc)
#data=sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('testdata.csv')
datardd=sc.textFile('testdata.csv').map(lambda x:x.split(','))

#############################ready data################################
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
def label(data):
    labelrdd=float(data[72])
    return labelrdd
def feature(data):
    featurerdd=Vectors.dense(data[1:72])
    return featurerdd
def get_data(datardd):
    head=datardd.first()
    rawdata=datardd.filter(lambda x:x!=head)
    labelpointrdd = rawdata.map(lambda x:LabeledPoint(label(x),feature(x)))
    (traindata,validationdata,testdata)=labelpointrdd.randomSplit([8,1,1])
    print('traindata:'+str(traindata.count())+'\n'+'validationdata:'+str(validationdata.count())+'\n'+'testdata:'+str(testdata.count()))
    return (traindata,validationdata,testdata)
traindata,validationdata,testdata=get_data(datardd)



#############################random forest#############################
#model Evaluation
from pyspark.mllib.tree import RandomForest
model=RandomForest.trainClassifier(traindata, numClasses=2, numTrees=3,categoricalFeaturesInfo={},featureSubsetStrategy='auto', impurity='gini', maxDepth=4, maxBins=32, seed=0)
def evaluation(model,validationdata):
    score=model.predict(validationdata.map(lambda x:x.features))
    scoreandlabel=score.zip(validationdata.map(lambda x:x.label))
    metrics=BinaryClassificationMetrics(scoreandlabel)
    metrics2 = MulticlassMetrics(scoreandlabel)
    AUC=metrics.areaUnderROC
    precision0=metrics2.precision(0.0)
    precision1=metrics2.precision(1.0)
    acc= metrics2.accuracy
    recall1=metrics2.recall(1.0)
    recall0=metrics2.recall(0.0)
    return (AUC,precision1,precision0,acc,recall1,recall0)

#param recommend
def trainevaluatemodel_tree(traindata,validationdata,numtrees,impurity,maxdepth,maxbins,seed):
    starttime=time()
    model=RandomForest.trainClassifier(traindata, numClasses=2, numTrees=numtrees,categoricalFeaturesInfo={},featureSubsetStrategy='auto', impurity=impurity, maxDepth=maxdepth, maxBins=maxbins,seed=seed)
    AUC=evaluation(model,validationdata)
    duration=time()-starttime
    print('Param:'+'\n'+'numtrees:'+str(numtrees)+'\n'+'impurity:'+str(impurity)+'\n'+'maxdepth:'+str(maxdepth)+'\n'+'maxbins:'+str(maxbins)+'\n'+'time:'+str(duration)+'\n'+'AUC:'+str(AUC))
    return (numtrees,impurity,maxdepth,maxbins,duration,AUC)
def evalparam_tree(traindata,validationdata,numtreeslist,impuritylist,maxdepthlist,maxbinslist,seed):
    metrics=[trainevaluatemodel_tree(traindata,validationdata,numtrees,impurity,maxdepth,maxbins,seed)
                for numtrees in numtreeslist
                for impurity in impuritylist
                for maxdepth in maxdepthlist
                for maxbins in maxbinslist]
    #print(metrics[2][5][0])
    Smetrics=sorted(metrics,key=lambda x:x[5][0],reverse=True)
    bestparam=Smetrics[0]
    #print('Param:'+'\n'+'numtrees:'+str(bestparam[0])+'\n'+'impurity:'+str(bestparam[1])+'\n'+'maxdepth:'+str(bestparam[2])+'\n'+'maxbins:'+str(bestparam[3])+'\n'+'time:'+str(bestparam[4])+'\n'+'AUC:'+str(bestparam[5]))
    return bestparam
evalparam_tree(traindata,validationdata,[4,5],['gini','entropy'],[3,4],[10,20],0)



#####################################GBDT#############################
#model Evaluation
from pyspark.mllib.tree import GradientBoostedTrees
model2=GradientBoostedTrees.trainClassifier(traindata, categoricalFeaturesInfo={}, loss='logLoss', numIterations=100, learningRate=0.001, maxDepth=3, maxBins=32)
evaluation(model2,validationdata)

#param recommend
def trainevaluatemodel_gbdt(traindata,validationdata,loss,numiterations,learningrate,maxdepth,maxbins):
    starttime=time()
    model=GradientBoostedTrees.trainClassifier(traindata, categoricalFeaturesInfo={}, loss=loss, numIterations=numiterations, learningRate=learningrate,maxDepth=maxdepth, maxBins=maxbins)
    index=evaluation(model,validationdata)
    duration=time()-starttime
    print('Param:'+'\n'+'loss:'+str(loss)+'\n'+'numiterations:'+str(numiterations)+'\n'+'learningrate:'+str(learningrate)+'\n'+'maxdepth:'+str(maxdepth)+'\n'+'maxbins:'+str(maxbins)+'\n'+'time:'+str(duration)+'\n'+'index:'+str(index))
    return (loss,numiterations,learningrate,maxdepth,maxbins,duration,index)
def evalparam_gbdt(traindata,validationdata,losslist,numiterationslist,learningratelist,maxdepthlist,maxbinslist):
    metrics=[trainevaluatemodel_gbdt(traindata,validationdata,loss,numiterations,learningrate,maxdepth,maxbins)
                for loss in losslist
                for numiterations in numiterationslist
                for learningrate in learningratelist
                for maxdepth in maxdepthlist
                for maxbins in maxbinslist]
    Smetrics=sorted(metrics,key=lambda x:x[6][0],reverse=True)
    bestparam=Smetrics[0]
    #print('Param:'+'\n'+'numtrees:'+str(bestparam[0])+'\n'+'impurity:'+str(bestparam[1])+'\n'+'maxdepth:'+str(bestparam[2])+'\n'+'maxbins:'+str(bestparam[3])+'\n'+'time:'+str(bestparam[4])+'\n'+'AUC:'+str(bestparam[5]))
    return bestparam
evalparam_gbdt(traindata,validationdata,['logLoss','leastSquaresError','leastAbsoluteError'],[3,4],[0.001,0.01],[3,4],[10,5])



#########################logistic###########################################
#model Evaluation
from pyspark.mllib.classification import LogisticRegressionWithSGD
model3=LogisticRegressionWithSGD.train(traindata, iterations=100, step=1.0, miniBatchFraction=1.0, initialWeights=None, regParam=0.01, regType='l2', intercept=False, validateData=True, convergenceTol=0.001)
def evaluation2(model,validationdata):
    score=model.predict(validationdata.map(lambda x:x.features))
    scoreandlabel=score.map(lambda x:float(x)).zip(validationdata.map(lambda x:x.label))
    metrics=BinaryClassificationMetrics(scoreandlabel)
    metrics2 = MulticlassMetrics(scoreandlabel)
    AUC=metrics.areaUnderROC
    precision0=metrics2.precision(0.0)
    precision1=metrics2.precision(1.0)
    acc= metrics2.accuracy
    recall1=metrics2.recall(1.0)
    recall0=metrics2.recall(0.0)
    return (AUC,precision1,precision0,acc,recall1,recall0)
evaluation2(model3,validationdata)

#param recommend
def trainevaluatemodel_logit(model,traindata,validationdata, iterations, step, minibatchfraction,regparam):
    starttime=time()
    model=LogisticRegressionWithSGD.train(traindata, iterations=iterations, step=step, miniBatchFraction=minibatchfraction, initialWeights=None, regParam=regparam, regType='l2', intercept=False, validateData=True, convergenceTol=0.001)
    index=evaluation2(model,validationdata)
    duration=time()-starttime
    print('Param:'+'\n'+'iterations:'+str(iterations)+'\n'+'step:'+str(step)+'\n'+'minibatchfraction:'+str(minibatchfraction)+'\n'+'regparam:'+str(regparam)+'\n'+'time:'+str(duration)+'\n'+'index:'+str(index))
    return (iterations, step, minibatchfraction,regparam,duration,index)
def evalparam_logit(traindata,validationdata, iterationslist, steplist, minibatchfractionlist,regparamlist):
    metrics=[trainevaluatemodel_logit(traindata,validationdata, iterations, step, minibatchfraction,regparam)
                for iterations in iterationslist
                for step in steplist
                for minibatchfraction in minibatchfractionlist
                for regparam in regparamlist]
    #print(metrics[5][5][0])
    Smetrics=sorted(metrics,key=lambda x:x[5][0],reverse=True)
    bestparam=Smetrics[0]
    #print('Param:'+'\n'+'numtrees:'+str(bestparam[0])+'\n'+'impurity:'+str(bestparam[1])+'\n'+'maxdepth:'+str(bestparam[2])+'\n'+'maxbins:'+str(bestparam[3])+'\n'+'time:'+str(bestparam[4])+'\n'+'AUC:'+str(bestparam[5]))
    return bestparam
evalparam_logit(traindata,validationdata, [10,15], [0.1,1], [0.5,1],[0.5,2.0])



###################################SVM#################################
from pyspark.mllib.classification import SVMWithSGD
model4=SVMWithSGD.train(traindata,iterations=100, step=1.0, regParam=0.01, miniBatchFraction=1.0, initialWeights=None, regType='l2', intercept=False, validateData=True, convergenceTol=0.001)
evaluation2(model4,validationdata)

def trainevaluatemodel_svm(traindata,validationdata, iterations, step, minibatchfraction,regparam):
    starttime=time()
    model=SVMWithSGD.train(traindata,iterations=iterations, step=step, regParam=regparam, miniBatchFraction=minibatchfraction, initialWeights=None, regType='l2', intercept=False, validateData=True, convergenceTol=0.001)
    index=evaluation2(model,validationdata)
    duration=time()-starttime
    print('Param:'+'\n'+'iterations:'+str(iterations)+'\n'+'step:'+str(step)+'\n'+'minibatchfraction:'+str(minibatchfraction)+'\n'+'regparam:'+str(regparam)+'\n'+'time:'+str(duration)+'\n'+'index:'+str(index))
    return (iterations, step, minibatchfraction,regparam,duration,index)
def evalparam_svm(traindata,validationdata, iterationslist, steplist, minibatchfractionlist,regparamlist):
    metrics=[trainevaluatemodel_svm(traindata,validationdata, iterations, step, minibatchfraction,regparam)
                for iterations in iterationslist
                for step in steplist
                for minibatchfraction in minibatchfractionlist
                for regparam in regparamlist]
    #print(metrics[5][5][0])
    Smetrics=sorted(metrics,key=lambda x:x[5][0],reverse=True)
    bestparam=Smetrics[0]
    #print('Param:'+'\n'+'numtrees:'+str(bestparam[0])+'\n'+'impurity:'+str(bestparam[1])+'\n'+'maxdepth:'+str(bestparam[2])+'\n'+'maxbins:'+str(bestparam[3])+'\n'+'time:'+str(bestparam[4])+'\n'+'AUC:'+str(bestparam[5]))
    return bestparam
evalparam_svm(traindata,validationdata, [10,15], [0.1,1], [0.5,1],[0.5,2.0])
