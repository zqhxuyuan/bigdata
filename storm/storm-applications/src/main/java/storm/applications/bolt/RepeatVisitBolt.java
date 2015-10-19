package storm.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;

import java.util.Map;
import static storm.applications.constants.ClickAnalyticsConstants.*;

/**
 * User: domenicosolazzo
 */
public class RepeatVisitBolt extends AbstractBolt {
    private Map<String, Void> map;

    @Override
    public void initialize() {
        map = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String clientKey = input.getStringByField(Field.CLIENT_KEY);
        String url = input.getStringByField(Field.URL);
        String key = url + ":" + clientKey;

        //第一条一定是不唯一的. 第二条之后的,如果是重复的记录,则每一条都表示不是唯一的
        //每一条记录都会发送一次. 在统计不唯一的时候,只要统计最后一个字段表示不唯一的
        //在统计所有记录的时候, 则不管最后一个字段是不是唯一的.只要有记录进来,就统计一次

        //如果有重复的key, 则再发送一次, 表示自己是不唯一的
        if (map.containsKey(key)) {
            collector.emit(input, new Values(clientKey, url, Boolean.FALSE.toString()));
        } else {
            //第一条记录, 会往Map中放入key,并发送表示自己是唯一的
            map.put(key, null);
            collector.emit(input, new Values(clientKey, url, Boolean.TRUE.toString()));
        }
        
        collector.ack(input);
    }

    //TRUE表示UNIQUE, 即一个客户端只访问了一个资源,不会重复访问这个资源
    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.CLIENT_KEY, Field.URL, Field.UNIQUE);
    }
}
