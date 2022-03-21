#创建xcache对象
import asyncio
import aioredis
import XCache as XCache
import Env
env = Env.Test
if env in (Env.Dev, Env.Test):
    XCACHE_ZK_NODE = '/zk/codis/db_xima-test/proxy'
elif env == Env.Uat:
    XCACHE_ZK_NODE = '/zk/codis/db_ximalayatest1/proxy'
elif env == Env.Prod:
    XCACHE_ZK_SERVER = 'sh-nh-b2-201-x12-hadoop-57-200:2181,sh-nh-b2-201-x13-hadoop-57-201:2181,sh-nh-b2-201-x14-hadoop-57-202:2181'
    XCACHE_ZK_NODE = '/zk/codis/db_xima-asr/proxy'
xcache = XCache(env, config.XCACHE_ZK_NODE)
xcache.clusternodes()
async def main():
    xcache = XCache(Env.Test)


if __name__ == "__main__":
    asyncio.run(main())