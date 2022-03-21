#coding=utf8
''''
author：welili@xmly.com
time:2022/3/16
func:清理redis数据库老旧的键
'''

import redis
import datetime
pool = redis.ConnectionPool(host='192.168.60.48',port=19153)
#r=StrictRedis(host='192.168.60.48',port=19153)  #建立连接
r=redis.Redis(connection_pool=pool)

def del_task_key_with_pipe():
    result_length=0
    pipe=r.pipeline()#写
    for key in r.scan_iter(match='task*',count=5000):
        pipe.delete(key)
        result_length+=1
        if result_length % 5000 ==0:
            pipe.execute()
    pipe.execute()#执行

def del_hiasr_upload_id_key_with_pipe():
    result_length=0
    pipe=r.pipeline()#写
    for key in r.scan_iter(match='hiasr:upload-id*',count=5000):
        pipe.delete(key)
        result_length+=1
        if result_length % 5000 ==0:
            pipe.execute()
    pipe.execute()#执行
def del_hiasr_audio_url_key_with_pipe():
    result_length=0
    pipe=r.pipeline()#写
    for key in r.scan_iter(match='hiasr:audio-url*',count=5000):
        pipe.delete(key)
        result_length+=1
        if result_length % 5000 ==0:
            pipe.execute()
    pipe.execute()#执行

def del_hiasr_create_date_key_with_pipe():
    result_length=0
    pipe=r.pipeline()#写
    for key in r.scan_iter(match='hiasr:create-date*',count=5000):
        pipe.delete(key)
        result_length+=1
        if result_length % 5000 ==0:
            pipe.execute()
    pipe.execute()#执行

def del_hiasr_app_name_key_with_pipe():
    result_length=0
    pipe=r.pipeline()#写
    for key in r.scan_iter(match='hiasr:app-name*',count=5000):
        pipe.delete(key)
        result_length+=1
        if result_length % 5000 ==0:
            pipe.execute()
    pipe.execute()#执行

def del_hiasr_status_key_with_pipe():
    result_length=0
    pipe=r.pipeline()#写
    for key in r.scan_iter(match='hiasr:status*',count=5000):
        pipe.delete(key)
        result_length+=1
        if result_length % 5000 ==0:
            pipe.execute()
    pipe.execute()#执行


def del_key1_with_pipe():
    result_length=0
    pipe=r.pipeline()#写
    #scan 指定的key，并解析key中的年月日
    for key in r.scan_iter(match='hiasr:create-time:*',count=5000):# scan hiasr:create-time:*
        parts = key.split(':')
        if parts[0] == 'hiasr' and parts[1] == 'create-time':
            year_redis = parts[2]
            month_redis = parts[3]
            day_redis = parts[4]
        # 计算两个日期相差的天数
        date_now=datetime.date.today()
        year_now_year=date_now.year
        year_now_month=date_now.month
        year_now_day=date_now.day
        date_redis= year_redis,month_redis,day_redis      #如何获取year,month,day
        d1=datetime.datetime(year_now_year,year_now_month,year_now_month)
        d2=datetime.datetime(year_redis,month_redis,day_redis)
        number=(d1-d2).days
        if number > 90:
            pipe.delete(key)
            result_length+=1
            if result_length % 5000 ==0:
                pipe.execute()
    pipe.execute()#执行
'''
1、遍历k2,k3中的元素，存到一个元组里面
2、遍历k1中的元素
3、用for i not in k2te  and k3te:
4、删除key4=f'hiasr:task:{i}'
'''
def del_key4_with_pipe():
    key_values_k2=[]
    key_values_k3=[]
    result_length=0
    pipe=r.pipeline()#写
    #1
    for key in r.scan_iter(match='hiasr:album-id*',count=5000):
        key_values_k2=pipe.get('key')
        result_length+=1
        if result_length % 5000 ==0:
            pipe.execute()
    #2
    for key in r.scan_iter(match='hiasr:track-id*', count=5000):
        key_values_k3 = pipe.get('key')
        result_length += 1
        if result_length % 5000 == 0:
            pipe.execute()
    #3
    for key in r.scan_iter(match='hiasr:create-time:*',count=5000):
        if  key not in key_values_k2 and key_values_k3:
            for key4 in r.scan_iter(match='hiasr:task:*',count=5000):
                pipe.delete(key4)  #4
    pipe.execute()#执行


if __name__ == '__main__':
  pass