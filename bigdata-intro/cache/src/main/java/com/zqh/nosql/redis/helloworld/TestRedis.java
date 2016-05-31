package com.zqh.nosql.redis.helloworld;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.util.Hashing;

import java.nio.charset.Charset;
import java.util.*;

public class TestRedis {

    private Jedis jedis;

    public static final String SUBSCRIBE_CENTER = "_-subscribe-center-_";

    public static final String MESSAGE_TXID = "_-message-txid-_";

    @Before
    public void setup() {
        //jedis = new Jedis("172.17.212.73", 6379);
        //jedis.auth("admin");
        jedis = new Jedis("192.168.6.52", 6379);
    }

    /**
     * redis存储字符串
     */
    @Test
    public void testString() {
        //-----添加数据----------
        jedis.set("name","xinxin");//向key-->name中放入了value-->xinxin
        System.out.println(jedis.get("name"));//执行结果：xinxin

        jedis.append("name", " is my lover"); //拼接
        System.out.println(jedis.get("name"));

        jedis.del("name");  //删除某个键
        System.out.println(jedis.get("name"));
        //设置多个键值对
        jedis.mset("name","liuling","age","23","qq","476777389");
        jedis.incr("age"); //进行加1操作
        System.out.println(jedis.get("name") + "-" + jedis.get("age") + "-" + jedis.get("qq"));
    }

    /**
     * redis操作Map
     */
    @Test
    public void testMap() {
        //-----添加数据----------
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "xinxin");
        map.put("age", "22");
        map.put("qq", "123456");
        jedis.hmset("userMap",map);
        //取出user中的name，执行结果:[minxr]-->注意结果是一个泛型的List
        //第一个参数是存入redis中map对象的key，后面跟的是放入map中的对象的key，后面的key可以跟多个，是可变参数
        List<String> rsmap = jedis.hmget("userMap", "name", "age", "qq");
        System.out.println(rsmap);

        //删除map中的某个键值
        jedis.hdel("userMap","age");
        System.out.println(jedis.hmget("userMap", "age")); //因为删除了，所以返回的是null
        System.out.println(jedis.hlen("userMap")); //返回key为user的键中存放的值的个数2
        System.out.println(jedis.exists("userMap"));//是否存在key为user的记录 返回true
        System.out.println(jedis.hkeys("userMap"));//返回map对象中的所有key
        System.out.println(jedis.hvals("userMap"));//返回map对象中的所有value

        Iterator<String> iter=jedis.hkeys("userMap").iterator();
        while (iter.hasNext()){
            String key = iter.next();
            System.out.println(key+":"+jedis.hmget("userMap",key));
        }
    }

    /**
     * jedis操作List
     */
    @Test
    public void testList(){
        //开始前，先移除所有的内容
        jedis.del("java framework");
        System.out.println(jedis.lrange("java framework",0,-1));
        //先向key java framework中存放三条数据
        jedis.lpush("java framework","spring");
        jedis.lpush("java framework","struts");
        jedis.lpush("java framework","hibernate");
        //再取出所有数据jedis.lrange是按范围取出，
        //第一个是key，第二个是起始位置，第三个是结束位置，jedis.llen获取长度 -1表示取得所有
        System.out.println(jedis.lrange("java framework",0,-1));

        jedis.del("java framework");
        jedis.rpush("java framework","spring");
        jedis.rpush("java framework","struts");
        jedis.rpush("java framework","hibernate");
        System.out.println(jedis.lrange("java framework",0,-1));
    }

    /**
     * jedis操作Set
     */
    @Test
    public void testSet(){
        //添加
        jedis.sadd("userSet","liuling","xinxin","ling");
        //移除noname
        jedis.srem("userSet","who");
        System.out.println(jedis.smembers("userSet"));//获取所有加入的value
        System.out.println(jedis.sismember("userSet", "who"));//判断 who 是否是user集合的元素
        System.out.println(jedis.srandmember("userSet"));
        System.out.println(jedis.scard("userSet"));//返回集合的元素个数
    }

    @Test
    public void testSort() throws InterruptedException {
        //jedis 排序
        //注意，此处的rpush和lpush是List的操作。是一个双向链表（但从表现来看的）
        jedis.del("a");//先清除数据，再加入数据进行测试
        jedis.rpush("a", "1");
        jedis.lpush("a","6");
        jedis.lpush("a","3");
        jedis.lpush("a","9");
        System.out.println(jedis.lrange("a",0,-1));// [9, 3, 6, 1]
        System.out.println(jedis.sort("a")); //[1, 3, 6, 9]  //输入排序后结果
        System.out.println(jedis.lrange("a",0,-1));
    }

    // http://shift-alt-ctrl.iteye.com/blog/1863790
    @Test
    public void testPipeline(){
        String key = "pipeline-test";
        String old = jedis.get(key);
        if(old != null){
            System.out.println("Key:" + key + ",old value:" + old);
        }

        Pipeline p1 = jedis.pipelined();
        p1.incr(key);
        p1.incr(key);
        //结束pipeline，并开始从响应中获得数据
        List<Object> responses = p1.syncAndReturnAll();
        if(responses == null || responses.isEmpty()){
            throw new RuntimeException("Pipeline error: no response...");
        }
        for(Object resp : responses){
            System.out.println("Response:" + resp.toString());//注意，此处resp的类型为Long
        }

        Pipeline p2 = jedis.pipelined();
        Response<Long> r1 = p2.incr(key);
        try{
            r1.get();
        }catch(Exception e){
            System.out.println("Error,you cant get() before sync,because IO of response hasn't begin..");
        }
        Response<Long> r2 = p2.incr(key);
        p2.sync();
        System.out.println("Pipeline,mode 2,--->" + r1.get());
        System.out.println("Pipeline,mode 2,--->" + r2.get());
    }

    @Test
    public void testTxPipeline(){
        String key = "pipeline-test";
        String old = jedis.get(key);
        if(old != null){
            System.out.println("Key:" + key + ",old value:" + old);
        }

        Pipeline p1 = jedis.pipelined();
        p1.multi(); // 开启事务
        p1.incr(key);
        p1.incr(key);
        //Cannot use Jedis when in Multi. Please use JedisTransaction instead.
        //System.out.println(jedis.get(key));
        //jedis.set("txptest", "1");
        Response<List<Object>> txresult= p1.exec(); //提交事务
        p1.sync();

        List<Object> responses = txresult.get();
        if(responses == null || responses.isEmpty()){
            throw new RuntimeException("Pipeline error: no response...");
        }
        for(Object resp : responses){
            System.out.println("Response:" + resp.toString());//注意，此处resp的类型为Long
        }
    }

    @Test
    public void testTransaction(){
        String key = "transaction-key";
        jedis.set(key, "20");

        jedis.watch(key);
        Transaction tx = jedis.multi();
        tx.incr(key);
        tx.incr(key);
        tx.incr(key);
        List<Object> result = tx.exec();

        if(result == null || result.isEmpty()){
            System.out.println("Transaction error...");//可能是watch-key被外部修改，或者是数据操作被驳回
            return;
        }
        for(Object rt : result){
            System.out.println(rt.toString());
        }
    }

    // http://shift-alt-ctrl.iteye.com/blog/1867454
    @Test
    public void testPubSub() throws Exception{
        String channel = "pubsub-channel";

        Jedis pubClient = new Jedis("localhost", 6379);
        pubClient.publish(channel, "before1");
        pubClient.publish(channel, "before2");
        Thread.sleep(2000);

        //消息订阅者非常特殊，需要独占链接，因此我们需要为它创建新的链接；
        //此外，jedis客户端的实现也保证了“链接独占”的特性，subscribe方法将一直阻塞，直到调用listener.unsubscribe方法
        Thread subThread = new Thread(new Runnable() {
            @Override
            public void run() {
                Jedis subClient = new Jedis("localhost", 6379);
                JedisPubSub listener = new PrintListener();
                //在API级别，此处为轮询操作，直到unsubscribe调用，才会返回
                //此处将会阻塞，在client代码级别为JedisPubSub在处理消息时，将会“独占”链接 并且采取了while循环的方式，侦听订阅的消息
                subClient.subscribe(listener, channel);
            }
        });
        subThread.start();

        int i=0;
        while(i < 10){
            String message = RandomStringUtils.random(6, true, true);//apache-commons
            pubClient.publish(channel, message);
            i++;
            Thread.sleep(1000);
        }

        //被动关闭指示，如果通道中，消息发布者确定通道需要关闭，那么就发送一个“quit”
        //那么在listener.onMessage()中接收到“quit”时，其他订阅client将执行“unsubscribe”操作。
        jedis.publish(channel, "quit");
        jedis.del(channel);

        //listener.unsubscribe(channel);  //此外，你还可以这样取消订阅
        subThread.interrupt();
    }

    @Test
    public void testPersistPubSub() throws Exception{
        String channel = "pubsub-channel-p";

        Jedis pubClient = new Jedis("localhost", 6379);
        Jedis subClient = new Jedis("localhost", 6379);
        JedisPubSub listener = new PPrintListener("subClient-1", new Jedis("localhost", 6379));

        Thread subThread = new Thread(new Runnable() {
            @Override
            public void run() {
                subClient.subscribe(listener, channel);
            }
        });
        subThread.setDaemon(true);
        subThread.start();

        int i = 0;
        while(i < 2){
            String message = RandomStringUtils.random(6, true, true);//apache-commons
            pubClient.publish(channel, message);
            i++;
            Thread.sleep(1000);
        }
        listener.unsubscribe(channel);
    }

    // http://shift-alt-ctrl.iteye.com/blog/1869996
    @Test
    public void testCommunicateSocketClient(){
        SocketClient client = new SocketClient("127.0.0.1", 6379);
        client.set("testset", "012xyz中国_？");
        System.out.println(client.get("testset"));
        List<String> list = client.lrange("testlist", 0, -1);
        for(String item : list){
            System.out.println("--:" + item);
        }
        System.out.println("incr:" + client.incr("testincr"));
    }

    @Test
    public void testPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(1024);
        config.setMaxIdle(200);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);

        JedisPool jedisPool = new JedisPool(config, "localhost", 6379, 1500);
        Jedis myJedis = jedisPool.getResource();
        myJedis.set("newname", "中文测试");
        System.out.println(myJedis.get("newname"));

        jedisPool.returnResource(myJedis);
    }

    @Test
    public void testShard(){
        //ip,port,timeout,weight
        JedisShardInfo si1 = new JedisShardInfo("127.0.0.1", 6379,15000,1);
        JedisShardInfo si2 = new JedisShardInfo("127.0.0.1", 6479,15000,1);
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        shards.add(si1);
        shards.add(si2);
        //指定hash算法,默认为"cache-only"的一致性hash,不过此处InnerHashing为强hash分片
        ShardedJedis shardedJedis = new ShardedJedis(shards,new InnerHashing());
        shardedJedis.set("k1", "v1");
        Charset charset = Charset.forName("utf-8");
        //注意此处对key的字节转换时，一定要和Innerhashing.hash(String)保持一致
        System.out.println(shardedJedis.get("k1").getBytes(charset));
    }
    class InnerHashing implements Hashing {
        Charset charset = Charset.forName("utf-8");
        @Override
        public long hash(String key) {
            return hash(key.getBytes(charset));
        }
        @Override
        public long hash(byte[] key) {
            int hashcode = new HashCodeBuilder().append(key).toHashCode();
            return hashcode & 0x7FFFFFFF;
        }
    }

    @Test
    public void testShardAndPool(){
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(6);
        config.setMinIdle(0);

        JedisShardInfo si1 = new JedisShardInfo("127.0.0.1", 6379,15000,1);
        JedisShardInfo si2 = new JedisShardInfo("127.0.0.1", 6479,15000,1);
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
        shards.add(si1);
        shards.add(si2);

        ShardedJedisPool sjp = new ShardedJedisPool(config, shards, new InnerHashing());
        ShardedJedis shardedJedis = sjp.getResource();
        System.out.println(shardedJedis.get("k1"));
        sjp.returnResource(shardedJedis);
    }
}
