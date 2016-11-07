package org.calrissian.flowmix.api;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.calrissian.flowmix.core.support.EventSortByComparator;
import org.calrissian.flowmix.core.support.deque.LimitingDeque;
import org.calrissian.flowmix.core.support.deque.LimitingPriorityDeque;
import org.calrissian.flowmix.core.support.window.Window;
import org.calrissian.flowmix.core.support.window.WindowItem;
import org.calrissian.flowmix.example.support.MockEvent;
import org.calrissian.mango.domain.Pair;
import org.calrissian.mango.domain.Tuple;
import org.calrissian.mango.domain.event.BaseEvent;
import org.calrissian.mango.domain.event.Event;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Created by zhengqh on 15/9/8.
 */
public class EventTupleTest {

    private Collection<Event> getSimpleEvents() {
        Collection<Event> eventCollection = new ArrayList<Event>();

        Event event = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));
        event.put(new Tuple("key3", "val3"));
        event.put(new Tuple("key4", "val4"));
        event.put(new Tuple("key5", "val5"));

        eventCollection.add(event);
        return eventCollection;
    }

    @Test
    public void testWindowEvent(){
        Window window = new Window("", 10);
        window.add(new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis()), "");
        window.add(new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis()), "");
        window.add(new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis()), "");
        for(WindowItem item : window.getEvents()){
            System.out.println(item.getEvent());
        }
        WindowItem last = window.expire();
        System.out.println(last.getEvent());
    }

    @Test
    public void testRepeatTupleKey(){
        Event event = new BaseEvent(UUID.randomUUID().toString(), System.currentTimeMillis());
        event.put(new Tuple("key1", "val1"));
        event.put(new Tuple("key2", "val2"));
        event.put(new Tuple("key3", "val3"));

        //重复的key. 和Map不一样,不会发生覆盖. 而是将Tuple中相同的key作为整体的key, 不同的value的Tuple作为整体的value
        //比如key1 -> [Tuple(key1,val1), Tuple(key1,val11)]
        //我们可以认为在同一时刻, 有多条事件进来.
        event.put(new Tuple("key1", "val11"));

        Collection<Tuple> tuples = event.getAll("key1");
        assertEquals(2, tuples.size());

        //相同的key和value. 照理说应该size=1. 但是实际上为2. 导致的问题是: JoinBoltIT中的assertEquals(1, event.getAll("key1").size());失败.
        event.put(new Tuple("key2", "val2"));
        Collection<Tuple> tuples2 = event.getAll("key2");
        System.out.println(tuples2);
        System.out.println(tuples2.size());
        //assertEquals(1, tuples.size());
    }

    @Test
    public void testRandom(){
        Random rand = new Random(5);
        int val = 0;
        int less = 0;
        for(int i=0;i<100;i++){
            if(rand.nextInt()>=3){
                val++;
            }else{
                less++;
            }
        }
        System.out.println(val + "," + less);
    }

    @Test
    public void testMockRandomEvent(){
        for(int i=0;i<20;i++){
            Event event1 = MockEvent.mockOneEvent();
            System.out.println(event1.get("key3"));
        }
    }

    @Test
    public void testNullPartition(){
        String key = null;
        Map<String,Integer> map = new HashMap<>();
        map.put(key, 1);
    }

    /**
     * event-key3:3
     event-key3:0
     event-key3:3
     event-key3:0
     event-key3:6
     event-key3:6
     event-key3:7
     event-key3:8
     event-key3:1
     event-key3:4
     ---------------
     BaseEvent{type='', id='d2fb346c-6668-4c31-9f22-afdeed50179a', timestamp=1442415827524, tuples=[Tuple{key='key3', value=8, metadata={}}]}
     可以看到删除的最后一条记录并不是最后加入的key3=4的. 而是key3值最大的!
     因为按照升序排列, 最大的值在最后, 所以删除最后的元素时, 删除的是排序值最大的事件!
     */
    @Test
    public void testComparatorEvent(){
        List<Pair<String,Order>> sortBy = new ArrayList<>();
        sortBy.add(new Pair<>("key3",Order.ASC));
        Comparator<WindowItem> comparator = new EventSortByComparator(sortBy);
        Deque<WindowItem> events = new LimitingPriorityDeque<WindowItem>(10, comparator);

        for(int i=0;i<10;i++){
            Random rand = new Random();
            Event event1 = new BaseEvent();
            int value = rand.nextInt(10);
            System.out.println("event-key3:" + value);
            event1.put(new Tuple("key3",value));
            WindowItem item1 = new WindowItem(event1,System.currentTimeMillis(),"");
            events.add(item1);
        }
        System.out.println("---------------");
        WindowItem last = events.removeLast();
        System.out.println(last.getEvent());
    }

    //没有指定comparator时, 则按照添加到队列中的顺序, 删除最后一条记录即最后加入队列的事件.
    @Test
    public void testNonComparatorEvent(){
        Deque<WindowItem> events = new LimitingDeque<WindowItem>(10);

        for(int i=0;i<10;i++){
            Random rand = new Random();
            Event event1 = new BaseEvent();
            int value = rand.nextInt(10);
            System.out.println("event-key3:" + value);
            event1.put(new Tuple("key3",value));
            WindowItem item1 = new WindowItem(event1,System.currentTimeMillis(),"");
            events.add(item1);
        }
        System.out.println("---------------");
        WindowItem last = events.removeLast();
        System.out.println(last.getEvent());
    }

    @Test
    public void testCacheExpire() throws Exception{
        //Test1: 在失效时间之内都没有任何操作
        Cache<String,String> cacheWindow = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.SECONDS).build();
        cacheWindow.put("k","value");
        assertEquals("value", cacheWindow.getIfPresent("k"));

        Thread.sleep(5 * 1000);
        //在expireTime之间都没有发生任何操作, 则经过expire时间后, 获取不到window中的数据

        //取不到之前的数据了
        assertNull(cacheWindow.getIfPresent("k"));
        //但是cache对象是存在的
        assertNotNull(cacheWindow);

        printCache(cacheWindow);
    }

    public void printCache(Cache<String,String> cacheWindow){
        for(String str :  cacheWindow.asMap().values()){
            System.out.println(str);
        }
    }

    @Test
    public void testCache2() throws Exception{
        //Test1: 在失效时间之后发生了update操作
        Cache<String,String> cacheWindow = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.SECONDS).build();
        cacheWindow.put("k", "value");
        assertEquals("value", cacheWindow.getIfPresent("k"));

        Thread.sleep(5 * 1000);
        //放入相同的key, 相当于更新
        cacheWindow.put("k", "value2");

        assertEquals("value2", cacheWindow.getIfPresent("k"));

        printCache(cacheWindow);
    }

    @Test
    public void testCache3() throws Exception{
        Cache<String,String> cacheWindow = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.SECONDS).build();
        cacheWindow.put("k", "value");
        assertEquals("value", cacheWindow.getIfPresent("k"));

        Thread.sleep(5 * 1000);
        cacheWindow.put("k2", "value2"); //put diff key 放入不同的key. 相当于插入

        assertNull(cacheWindow.getIfPresent("k")); //Can we get 10s ago? sorry, it already gone!
        assertEquals("value2", cacheWindow.getIfPresent("k2"));

        printCache(cacheWindow);
    }
}
