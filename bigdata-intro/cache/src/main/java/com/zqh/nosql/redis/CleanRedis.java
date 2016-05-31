package com.td.bigdata.crosspartner;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by zhengqh on 16/2/24.
 */
public class CleanRedis {
    public static void main(String[] args) {
        testLocal();
    }

    public static void testAll(){
        long start = System.currentTimeMillis();
        long count = 0;
        Jedis jedis = Constant.getRedisInstant(Constant.ENV_PROD());

        // 遍历所有key删除
        String scanRet = "0";
        do {
            ScanResult<String> ret = jedis.scan(scanRet);
            scanRet = ret.getStringCursor();
            List<String> result = ret.getResult();
            Iterator<String> iterator = result.iterator();
            while(iterator.hasNext()){
                String key = iterator.next();
                //不能直接对所有的key进行同一个zrem操作,因为系统中已有的key的类型并不一定都是zset
                if(jedis.type(key).equals("zset")){
                    jedis.zrem(key, "tongdun_data");
                    System.out.println(key);
                    count ++;
                }
            }
        } while (!scanRet.equals("0"));

        jedis.close();
        long end = System.currentTimeMillis();
        System.out.println("Time:" + (end-start)/1000 + ",Count:" + count);
    }

    public static void testLocal(){
        Jedis jedis = Constant.getRedisInstant(Constant.ENV_LOCAL());
        String testKey = "mob_15959086950";

        // 模拟添加一条要删除的member
        jedis.zadd(testKey, 11111111113L, "tongdun_data");
        Set<String> res1 = jedis.zrange(testKey, 0, -1);
        for(String s : res1){
            System.out.println(s);
        }

        // 手工删除一条记录
        jedis.zrem(testKey, "tongdun_data");

        // 删除后验证数据
        res1 = jedis.zrange(testKey, 0, -1);
        for(String s : res1){
            System.out.println(s);
        }
        jedis.close();
    }
}
