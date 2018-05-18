# -*- coding: utf-8 -*-
"""
Project:Spark final exam

@author: Fan
"""

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

#dianxin,liantong,yidong = generate_head()  
    
def mean(Iterator):
    sums = 0
    cnt = len(Iterator)
    for item in Iterator:
        sums += item
    return sums/cnt
            
funcmode1 = lambda x:(x[0],1)
funcmode2 = lambda x,y:x+y