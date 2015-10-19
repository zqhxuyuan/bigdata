package storm.starter.util;

import backtype.storm.spout.SchemeAsMultiScheme;
import kafka.api.OffsetRequest;
import net.sf.json.JSONObject;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.starter.model.Dimension;
import storm.starter.tools.SlidingWindowCounter;

import java.util.*;

/**
 * Created by zhengqh on 15/9/21.
 */
public class WindowConstant {

    public static String splitKey = "::";
    public static String outputKey = "outputKey";
    public static String masterKey = "master";
    public static String timeUnitKey = "timeUnit";
    public static int paralize = 4;

    public static String topic = "demo_activity_event";
    public static String brokers = "192.168.6.55:9092,192.168.6.56:9092,192.168.6.57:9092";
    public static int zkPort = 2181;
    public static String zkConn = "192.168.6.55:2181,192.168.6.56:2181,192.168.6.57:2181";

    public static Properties kafkaProducerProp = new Properties(){{
        put("metadata.broker.list",brokers);
        put("serializer.class","kafka.serializer.StringEncoder");
    }};

    public static Map kafkaParams = new HashMap<String, String>(){{
        put("metadata.broker.list", brokers);
        put("serializer.class", "kafka.serializer.StringEncoder");
    }};

    public static List<String> zkHosts = new ArrayList<String>(){{
        add("192.168.6.55");
        add("192.168.6.56");
        add("192.168.6.57");
    }};

    public static String zkRoot = "/kafkastorm";
    public static String spoutId = "eventGenerator";  //读取的status会被存在，/kafkastorm/id下面，所以id类似consumer group

    //Kafka配置在zookeeper上的节点
    public static BrokerHosts brokerHosts = new ZkHosts(zkConn);
    public static SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
    static {
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = false;
        spoutConf.startOffsetTime = OffsetRequest.EarliestTime();
        spoutConf.metricsTimeBucketSizeInSecs = 6;

        //只有在local模式下需要记录读取状态时，才需要设置
        spoutConf.zkServers = zkHosts;
        spoutConf.zkPort = zkPort;
    }

    public static int min_1 = 60;
    public static int min_30 = 30*60;
    public static int hour_1 = 3600;
    public static int day_1 = hour_1 * 24;
    public static int day_3 = day_1 * 3;
    public static int day_7 = day_1 * 7;
    public static int mon_1 = day_1 * 30;
    public static int mon_3 = mon_1 * 3;
    public static int mon_6 = mon_1 * 6;

    public static List<Integer> timeUnits = new ArrayList(){{
        //add(10);              //10s
        add(60);                //1min
        add(30*60);             //30min
        add(3600);              //1hour
//        add(24*3600);           //1day
//        add(3*24*3600);         //3day
//        add(7*24*3600);         //7day
//        add(30*24*3600);        //1month
//        add(90*24*3600);        //3month
//        add(6*30*24*3600);      //6month
    }};

    public static final int NUM_WINDOW_CHUNKS = 5;
    public static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    //public static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    public static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 20;

    public static Map<Integer, SlidingWindowCounter<Object>> counterMap = new HashMap<>();

    public static Map<Integer, SlidingWindowCounter<Object>> initCounterMap(){
        for(Integer timeUnit : timeUnits) {
            counterMap.put(timeUnit, new SlidingWindowCounter<Object>(timeUnit / WindowConstant.DEFAULT_EMIT_FREQUENCY_IN_SECONDS));
        }
        return counterMap;
    }

    public static void debugJSON(JSONObject jsonObject){
        String str = System.currentTimeMillis()/1000 + ":" +  //tuple开始执行的时间
                jsonObject.getString(Dimension.partnerCode) + "," +
                jsonObject.getString(Dimension.accountLogin) + "," +
                jsonObject.getLong(Dimension.eventOccurTime)/1000; //事件真正发生的时间
        System.out.println(str);
    }
}
