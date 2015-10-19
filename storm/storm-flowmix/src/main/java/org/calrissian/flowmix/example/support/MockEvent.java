package org.calrissian.flowmix.example.support;

import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

/**
 * Created by zhengqh on 15/9/10.
 */
public class MockEvent {

    public static Collection<Event> getMockEvents() {
        Collection<Event> eventCollection = new ArrayList<Event>();

        //创建一条事件,只有UUID和时间撮
        Event event = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis());

        //往事件中填充信息. Key-Value键值对, event看起来像Map. 或者如果用JSON表示:
        //{id:f171edb8-a393-4eaf-bbc0-8fece8f1a22b, timestamp:1441637681000, {key1:val1, key2:val2, key3:val3}}
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));
        event.put(new Tuple("key3", "val3"));
        event.put(new Tuple("key4", "val4"));
        event.put(new Tuple("key5", "val5"));

        //在一条事件中相同key有不同的value.而我们实际要的效果可能是在不同的event中相同key有不同value.用上面的方式模拟.
        //注意key1的值没有变化.而key3的值变化. 表示key1这个ip=val1,在不同的事件中,它的账户(key3)分别是val3和val-3
        /*
        event.put(new Tuple("key1", "val-1"));
        event.put(new Tuple("key3", "val-3"));
        */

        eventCollection.add(event);
        return eventCollection;
    }

    //不要把Random放在方法里面,那样多次调用,仍然是第一次的值!
    static Random ran = new Random(10);
    public static Event mockOneEvent(){
        Event event = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis());
        if(ran.nextInt(10)>5){
            event.put(new Tuple("key1", "val1"));
            event.put(new Tuple("key2", "val2"));
            event.put(new Tuple("key3", "val3"));
            event.put(new Tuple("key4", "val4"));
            event.put(new Tuple("key5", "val5"));
        }else{
            event.put(new Tuple("key1", "val1"));
            event.put(new Tuple("key2", "val-2"));
            event.put(new Tuple("key3", "val-3"));
            event.put(new Tuple("key4", "val-4"));
            event.put(new Tuple("key5", "val-5"));
        }
        return event;
    }

    //更加随机的事件输出
    public static Event mockRandomEvent(){
        Event event = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));
        event.put(new Tuple("key3", "val_" + ran.nextInt(10)));
        event.put(new Tuple("key4", "val4"));
        event.put(new Tuple("key5", "val5"));
        return event;
    }
}
