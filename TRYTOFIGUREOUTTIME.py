# -*- coding: utf-8 -*-
"""
Created on Sat May 26 15:03:25 2018

@author: Administrator
"""

#Try to figure out the time mode on data
CALL_RDD_DAY_CNT_VOICE = voicedata.map(lambda x:(x[4][0:2],1)).reduceByKeyLocally(funcmode2)
CALL_RDD_DAY_CNT_SMS = smsdata.map(lambda x:(x[4][0:2],1)).reduceByKeyLocally(funcmode2)
CALL_RDD_DAY_CNT_WA = wadata.map(lambda x:(x[7],1)).reduceByKeyLocally(funcmode2)

xy= [(int(x),CALL_RDD_DAY_CNT[x]) for x in CALL_RDD_DAY_CNT]
xy.sort()
y = [i[1] for i in xy[1:]]
'''
from matplotlib import pyplot as plt
plt.subplot(211)
plt.title("Call time per day")
for i in range(7):
    yi = y[i::7]
    plt.plot(yi,label = 'Day {}'.format(i+1))
    plt.legend(loc='lower right')
'''


plt.subplot(311)
plt.title('Call time weekly')
for i in range(0,45,7):
    yi = y[i:i+7]
    week = int(i/7)
    plt.plot(yi,label = 'Week {}'.format(week))
    plt.legend(loc='lower right')

xy= [(int(x),CALL_RDD_DAY_CNT_SMS[x]) for x in CALL_RDD_DAY_CNT_SMS]
xy.sort()
y = [i[1] for i in xy[:]]
plt.subplot(312)
plt.title('Text time weekly')
for i in range(0,45,7):
    yi = y[i:i+7]
    week = int(i/7)
    plt.plot(yi,label = 'Week {}'.format(week))
    plt.legend(loc='lower right')

xy= [(int(x),CALL_RDD_DAY_CNT_WA[x]) for x in CALL_RDD_DAY_CNT_WA if x!='NULL']
xy.sort()
y = [i[1] for i in xy[:]]
from matplotlib import pyplot as plt
plt.subplot(313)
plt.title("Web time per day")
for i in range(7):
    yi = y[i::7]
    plt.plot(yi,label = 'Day {}'.format(i+1))
    plt.legend(loc='lower right')