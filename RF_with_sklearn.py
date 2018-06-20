# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 19:19:28 2018

@author: Administrator
"""

def mergedata(path1,path2,savepath):
    data1 =  pd.read_csv(path1)
    if data1.columns[0]=='Unnamed: 0':
        del data1['Unnamed: 0']
    data2 =  pd.read_csv(path2)
    if data1.columns[0]=='Unnamed: 0':
        del data1['Unnamed: 0']
    del data2['Unnamed: 0']
    merge_data = pd.merge(data1,data2, on="id",how='right')
    merge_data = merge_data.fillna(0)
    merge_data.to_csv(savepath)
    
    
import pandas as pd  
import numpy as np  
from sklearn.cross_validation import train_test_split  
from sklearn.preprocessing import StandardScaler  
import matplotlib.pyplot as plt  
from sklearn.ensemble import RandomForestClassifier  
#导入数据  

datapath = 'all.csv'
import pandas as pd  
import numpy as np  
from sklearn.cross_validation import train_test_split  
from sklearn.preprocessing import StandardScaler  
import matplotlib.pyplot as plt  
from sklearn.ensemble import RandomForestClassifier  
from sklearn.feature_selection import SelectFromModel

df_all = pd.read_csv(datapath)  

#分割训练集合测试集  
X,y=df_all.iloc[:,1:-1].values,df_all.iloc[:,-1].values  
X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.2,random_state=0)  
#特征值缩放-标准化，决策树模型不依赖特征缩放  
#stdsc=StandardScaler()  
#X_train_std=stdsc.fit_transform(X_train)  
#X_test_std=stdsc.fit_transform(X_test)  
#随机森林评估特征重要性  
feat_labels=df_all.columns[1:-1]  
forest=RandomForestClassifier(n_estimators=4,max_depth=10,n_jobs=-1,random_state=0)  
forest.fit(X_train,y_train)  
score = forest.score(X_test,y_test) 
forest_y_score = forest.predict_proba(X_test)
importances=forest.feature_importances_  


model = SelectFromModel(forest, threshold = 0.004,prefit=True)
X_new = model.transform(X_train)
forest.fit(X_new,y_train)



indices=np.argsort(importances)[::-1]  
for f in range(X_train.shape[1]):  
    #给予10000颗决策树平均不纯度衰减的计算来评估特征重要性  
    print ("%2d) %-*s %f" % (f+1,30,feat_labels[indices[f]],importances[indices[f]]) )  
#可视化特征重要性-依据平均不纯度衰减  
plt.title('Feature Importance-RandomForest')  
plt.bar(range(X_train.shape[1]),importances[indices],color='lightblue',align='center')  
plt.xticks(range(X_train.shape[1]),feat_labels,rotation=90)  
plt.xlim([-1,X_train.shape[1]])  
plt.tight_layout()  
plt.show()

X_selected=forest.transform(X_train,threshold=0.004)



from sklearn import metrics  
from sklearn.preprocessing import label_binarize  
from sklearn.metrics import roc_curve, auc 
test_auc = metrics.roc_auc_score(y_test,forest_y_score[:,1])  
fpr, tpr, _ = roc_curve(y_test, forest_y_score[:,1]) 
roc_auc = auc(fpr, tpr)
plt.title('ROC Validation')  
plt.plot(fpr, tpr, 'b', label='AUC = %0.2f' % roc_auc)  
plt.legend(loc='lower right')  
plt.plot([0, 1], [0, 1], 'r--')  
plt.xlim([0, 1])  
plt.ylim([0, 1])  
plt.ylabel('True Positive Rate')  
plt.xlabel('False Positive Rate')  
plt.show()  

metrics.f1_score(y_test, forest_y_score[:,1], average='weighted') 