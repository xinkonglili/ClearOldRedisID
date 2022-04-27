# coding=utf8
''''
author：welili@xmly.com
time:2022/3/16
func:清理redis数据库老旧的键
'''
import asyncio
import logging
import time
from distutils.command.config import config
import redis
import datetime
from aioxcache import XCache, xcache
from concurrent.futures import ThreadPoolExecutor
from xmly_env import Env

'''
print("-----",str(xvalue))#<coroutine object Redis.execute_command at 0x10ab921c0>
ri= redis.Redis(host='192.168.60.48', port=19153, password="xmly123456",decode_responses=True)
port_host_dict={}
for i in ri.cluster('nodes'):
    host,port=i.split(':')
    port_host_dict[int(port)]=host
print(port_host_dict)
r= redis.Redis(host='192.168.60.48', port=9307, password="xmly123456",decode_responses=True)
   for k,v in port_host_dict.items():
        print(v,k)
    r=redis.Redis(host='v',port='k',password="xmly123456",decode_responses=True)
'''

logger = logging.getLogger(__name__)
_executor = ThreadPoolExecutor(1)

async def del_task_key_with_pipe(r):
    start_time = time.time()
    pipe = r.pipeline(transaction=False)
    # pipe=r.pipeline(transaction=False)#写
    result_length = 0
    for key in r.scan_iter(match='task:*', count=10000000):
        print("task:*开头的key：", key)
        pipe.delete(key)
        result_length += 1
        if result_length % 10000000 == 0:
            pipe.execute()
    pipe.execute()  # 执行pipline.execute()有返回值，是一个列表，返回值的True或False，代表执行成功或失败
    print("use task pipline time", time.time() - start_time)

async def del_hiasr_upload_id_key_with_pipe(r):
    start_time = time.time()
    pipe = r.pipeline(transaction=False)  # 写
    result_length = 0
    for key in r.scan_iter(match='hiasr:upload-id:*', count=100000):
        print("hiasr:upload-id:*开头的key：", key)
        pipe.delete(key)
        result_length += 1
        if result_length % 1000000 == 0:
            pipe.execute()
    print("use pipline time", time.time() - start_time)
    pipe.execute()  # 执行

async def del_hiasr_audio_url_key_with_pipe(r):
    start_time = time.time()
    pipe = r.pipeline(transaction=False)  # 写
    result_length = 0
    for key in r.scan_iter(match='hiasr:audio-url:*', count=5000000):
        print("hiasr:audio-url:*开头的key：", key)
        pipe.delete(key)
        result_length += 1
        if result_length % 5000000 == 0:
            pipe.execute()
    print("use audio_url_pipline time", time.time() - start_time)
    pipe.execute()  # 执行

async def del_hiasr_create_date_key_with_pipe(r):
    start_time = time.time()
    result_length = 0
    pipe = r.pipeline(transaction=False)  # 写
    for key in r.scan_iter(match='hiasr:create-date:*', count=5000000):
        print("hiasr:create-date:*开头的key：", key)
        pipe.delete(key)
        result_length += 1
        if result_length % 5000000 == 0:
            pipe.execute()
    print("use create_date pipline time", time.time() - start_time)
    pipe.execute()  # 执行

async def del_hiasr_app_name_key_with_pipe(r):
    start_time = time.time()
    result_length = 0
    pipe = r.pipeline(transaction=False)  # 写
    for key in r.scan_iter(match='hiasr:app-name:*', count=5000000):
        print("hiasr:app-name:*开头的key：", key)
        pipe.delete(key)
        result_length += 1
        if result_length % 5000000 == 0:
            pipe.execute()
    print("use app_name pipline time", time.time() - start_time)
    pipe.execute()  # 执行

async def del_hiasr_status_key_with_pipe(r):
    start_time = time.time()
    result_length = 0
    pipe = r.pipeline(transaction=False)  # 写
    for key in r.scan_iter(match='hiasr:status:*', count=5000000):
        print("hiasr:status:*开头的key：", key)
        print(key)
        pipe.delete(key)
        result_length += 1
        if result_length % 5000000 == 0:
            pipe.execute()
    print("use hiasr_status pipline time", time.time() - start_time)
    pipe.execute()  # 执行

# key1 = 'hiasr: create-time:{year}:{month}:{day}:{hour}:{minute}'
async def del_key1_with_pipe(r):
    start_time = time.time()
    result_length = 0
    pipe = r.pipeline(transaction=False)  # 写
    # scan 指定的key，并解析key中的年月日
    for key in r.scan_iter(match='hiasr:create-time:*', count=5000000):  # scan hiasr:create-time:*
        parts = key.split(':')
        if parts[0] == 'hiasr' and parts[1] == 'create-time':
            if parts[2] > '2022':
                Year_redis = parts[3]  # 952074
                Year_redis = int(Year_redis)
                Month_redis = int(parts[4])
                Day_redis = int(parts[5])
                # Day_redis = filter(str.isdigit,day_redis)
                # 计算两个日期相差的天数
                date_now = datetime.date.today()  # 2022-03-24
                year_now_year = date_now.year  # 2022--int类型
                year_now_month = date_now.month
                year_now_day = date_now.day
                # date_redis= year_redis,month_redis,day_redis      #如何获取year,month,day
                d1 = datetime.datetime(year_now_year, year_now_month, year_now_day)
                d2 = datetime.datetime(Year_redis, Month_redis, Day_redis)
                number = (d1 - d2).days
            else:
                Year_redis = parts[2]  # 952074
                Year_redis = int(Year_redis)
                Month_redis = int(parts[3])
                Day_redis = int(parts[4])
                # 计算两个日期相差的天数
                date_now = datetime.date.today()  # 2022-03-24
                year_now_year = date_now.year  # 2022--int类型
                year_now_month = date_now.month
                year_now_day = date_now.day
                d1 = datetime.datetime(year_now_year, year_now_month, year_now_day)
                d2 = datetime.datetime(Year_redis, Month_redis, Day_redis)
                number = (d1 - d2).days

        if number > 90:
            print("hiasr: create-time:*开头的key：", key)
            pipe.delete(key)
            result_length += 1
            if result_length % 5000000 == 0:
                pipe.execute()
    print("use create-time:year:mounth pipline time", time.time() - start_time)
    pipe.execute()  # 执行
'''
1、遍历k2,k3中的元素，存到一个元组里面
2、遍历k1中的元素
3、用for i not in k2te  and k3te:
4、删除key4=f'hiasr:task:{i}'
'''

async def del_key4_with_pipe(r):
    start_time = time.time()
    key_values_k2 = []
    key_values_k3 = []
    result_length = 0
    pipe = r.pipeline(transaction=False)  # 写
    # 1、遍历k2,k3中的元素，存到一个元组里面
    for key in r.scan_iter(match='hiasr:album-id:*', count=5000000):
        key_values_k2 = pipe.get(key)
        pipe.execute()
    # 2、遍历k1中的元素
    for key in r.scan_iter(match='hiasr:track-id:*', count=5000000):
        key_values_k3 = pipe.get(key)
        pipe.execute()
    # 3、用for i not in k2te  and k3te:
    for key in r.scan_iter(match='hiasr:create-time:*', count=5000000):
        if key not in key_values_k2 and key_values_k3:
            for key4 in r.scan_iter(match='hiasr:task:*'):
                print("Not hiasr:album-id:* and not in hiasr:track-id:*---hiasr:create-time:*开头的key：", key)
                pipe.delete(key4)  # 4
                result_length += 1
                if result_length % 5000000 == 0:
                    pipe.execute()
    pipe.execute()  # 执行
    print("正在清理key4：", time.time() - start_time)

async def test(xcache: XCache):
    return await xcache.execute_command('cluster nodes')

async def main():
    env = Env.Test
    if env in (Env.Dev, Env.Test):
        XCACHE_ZK_NODE = '/zk/codis/db_xima-test/proxy'
    elif env == Env.Uat:
        XCACHE_ZK_NODE = '/zk/codis/db_ximalayatest1/proxy'
    elif env == Env.Prod:
        XCACHE_ZK_SERVER = 'sh-nh-b2-201-x12-hadoop-57-200:2181,sh-nh-b2-201-x13-hadoop-57-201:2181,sh-nh-b2-201-x14-hadoop-57-202:2181'
        XCACHE_ZK_NODE = '/zk/codis/db_xima-asr/proxy'
    xcache = XCache(env, XCACHE_ZK_NODE)
    # xvalue = xcache.cluster('node')#获取后端的地址
    ret = await test(xcache)  # 拿到一个字典了
    '''
    ret: {
'192.168.60.48:9312@9312': {'node_id': '192.168.60.48:9312', 'flags': 'master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '0', 'epoch': '0', 'slots': [['0'], ['256', '511']], 'connected': True}, 
'192.168.60.48:9307@9307': {'node_id': '192.168.60.48:9307', 'flags': 'master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '0', 'epoch': '0', 'slots': [['1', '255'], ['1022']], 'connected': True},
'192.168.60.48:9313@9313': {'node_id': '192.168.60.48:9313', 'flags': 'master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '0', 'epoch': '0', 'slots': [['512', '767']], 'connected': True}, 
'192.168.60.48:9316@9316': {'node_id': '192.168.60.48:9316', 'flags': 'master', 'master_id': '-', 'last_ping_sent': '0', 'last_pong_rcvd': '0', 'epoch': '0', 'slots': [['768', '1021'], ['1023']], 'connected': True}}

    '''
    # 解析
    for key in ret.keys():  # 遍历字典的key
        ip_port1 = key.split('@')
        ip_port = ip_port1[0]
        ip = ip_port.split(':')[0]
        # print(ip)
        port = ip_port.split(':')[1]
        # print(port)
        r = redis.Redis(host=ip, port=port, password="xmly123456", decode_responses=True)
        await del_task_key_with_pipe(r)  # ok
        await del_hiasr_upload_id_key_with_pipe(r)  # ok
        await del_hiasr_audio_url_key_with_pipe(r)  # ok
        await del_hiasr_create_date_key_with_pipe(r)  # ok
        await del_hiasr_app_name_key_with_pipe(r)  # ok
        await del_hiasr_status_key_with_pipe(r)  # ok
        await del_key1_with_pipe(r)
        await del_key4_with_pipe(r)
        print("该服务器正在清理：", ip)
        print("该服务器正在清理", port)


if __name__ == '__main__':
    asyncio.run(main())
    print("success")
