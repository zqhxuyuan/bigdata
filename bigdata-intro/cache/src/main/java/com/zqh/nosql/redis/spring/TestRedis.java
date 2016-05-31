package com.zqh.nosql.redis.spring;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import redis.clients.jedis.*;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * http://shift-alt-ctrl.iteye.com/blog/1886831
 */
public class TestRedis {

    RedisTemplate redisTemplate;
    ClassPathXmlApplicationContext context;

    @Before
    public void setup() {
        context = new ClassPathXmlApplicationContext("classpath:redis.xml");
        redisTemplate = (RedisTemplate)context.getBean("jedisTemplate");
    }

    @Test
    public void testJedisSpring() throws Exception{
        // 连接池
        JedisPool jedisPool = (JedisPool)context.getBean("jedisPool");
        Jedis client = jedisPool.getResource();
        client.select(0);
        client.set("k1", "v1");
        System.out.println(client.get("k1"));
        jedisPool.returnResource(client);

        // 分片
        ShardedJedis shardedJedis = (ShardedJedis)context.getBean("shardedJedis");
        shardedJedis.set("k1", "v2");
        System.out.println(shardedJedis.get("k1"));

        // 分片和连接池
        ShardedJedisPool shardedJedisPool = (ShardedJedisPool)context.getBean("shardedJedisPool");
        shardedJedis = shardedJedisPool.getResource();
        shardedJedis.set("k1", "v2");
        System.out.println(shardedJedis.get("k1"));
        shardedJedisPool.returnResource(shardedJedis);

        // 分片和连接池+Pipline
        shardedJedisPool = (ShardedJedisPool)context.getBean("shardedJedisPool");
        shardedJedis = shardedJedisPool.getResource();

        ShardedJedisPipeline shardedJedisPipeline = new ShardedJedisPipeline();
        shardedJedisPipeline.setShardedJedis(shardedJedis);
        shardedJedisPipeline.set("k1", "v1");
        shardedJedisPipeline.set("k3", "v3");
        shardedJedisPipeline.get("k3");
        List<Object> results = shardedJedisPipeline.syncAndReturnAll();
        for(Object result : results){
            System.out.println(result.toString());
        }
        shardedJedisPool.returnResource(shardedJedis);
    }

    @Test
    public void testRedisTemplate(){
        ValueOperations<String, User> valueOper = redisTemplate.opsForValue();
        User u1 = new User("zhangsan",12);
        User u2 = new User("lisi",25);
        valueOper.set("u:u1", u1);
        valueOper.set("u:u2", u2);
        System.out.println(valueOper.get("u:u1").getName());
        System.out.println(valueOper.get("u:u2").getName());
    }

    static class User implements Serializable {
        private static final long serialVersionUID = -3766780183428993793L;
        private String name;
        private Date created;
        private int age;
        public User(){}
        public User(String name,int age){
            this.name = name;
            this.age = age;
            this.created = new Date();
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public Date getCreated() {
            return created;
        }
        public void setCreated(Date created) {
            this.created = created;
        }
        public int getAge() {
            return age;
        }
        public void setAge(int age) {
            this.age = age;
        }
    }

    // http://shift-alt-ctrl.iteye.com/blog/1887644
    @Test
    public void testQueue() throws Exception{
        RedisQueue<String> redisQueue = (RedisQueue)context.getBean("jedisQueue");
        redisQueue.pushFromHead("test:app");
        Thread.sleep(1000);
        redisQueue.pushFromHead("test:app");
        Thread.sleep(1000);
        redisQueue.destroy();
    }

    // http://shift-alt-ctrl.iteye.com/blog/1887700
    @Test
    public void testPubAndSub(){
        String channel = "user:topic";
        //其中channel必须为string，而且“序列化”策略也是StringSerializer
        //消息内容，将会根据配置文件中指定的valueSerializer进行序列化
        //本例中，默认全部采用StringSerializer
        //那么在消息的subscribe端也要对“发序列化”保持一致。
        redisTemplate.convertAndSend(channel, "from app 1");
    }
}
