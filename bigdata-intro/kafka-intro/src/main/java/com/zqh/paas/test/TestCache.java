package com.zqh.paas.test;

import com.zqh.paas.PaasException;
import com.zqh.paas.cache.ICache;
import com.zqh.paas.cache.impl.RedisCache;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.*;
import static com.zqh.paas.cache.CacheUtil.*;

/**
 * Created by zqhxuyuan on 15-3-9.
 */
public class TestCache {

    public static void main(String[] args) throws PaasException,InterruptedException {
        ApplicationContext ctx = new ClassPathXmlApplicationContext(new String[] { "paasContext.xml" });
        ICache cacheSv = (RedisCache) ctx.getBean("cacheSv");

        cacheSv.getIncrement("1");

        cacheSv.addItem("country", "weme");
        cacheSv.addItem("sex", "8");
        System.out.println(cacheSv.getItem("country"));
        System.out.println(cacheSv.getItem("sex"));

        while (true) {
            Thread.sleep(5000);
            System.out.println(cacheSv.getItem("country"));
            System.out.println(cacheSv.getItem("sex"));
        }
    }

    public void testCacheUtil() throws Exception{
        // add then get value
        addItem("country", "weme");
        addItem("sex", "8");
        addItem("country2", "weme2",4);
        System.out.println(getItem("country"));
        System.out.println(getItem("sex"));
        System.out.println(getItem("country2"));

        // expire
        Thread.sleep(1000);
        System.out.println(getItem("country"));
        System.out.println(getItem("sex"));
        System.out.println(getItem("countr2"));

        // delete then get empty
        delItem("country");
        delItem("sex");
        System.out.println(getItem("country"));
        System.out.println(getItem("sex"));

        Map<String,String> map1 = new HashMap<String,String>();
        map1.put("1", "a");
        map1.put("2", "b");
        map1.put("3", "c");
        map1.put("4", "d");
        Map<String,String> map2 = new LinkedHashMap<String,String>();
        map2.put("1", "a");
        map2.put("2", "b");
        map2.put("3", "c");
        map2.put("4", "d");
        for(String key:map2.keySet()){
            System.out.println(key+"----"+map2.get(key));
        }
        addMap("map1-liwx", map1);
        addMap("map2-liwx", map2);
        System.out.println("map1:"+getMap("map1-liwx"));
        System.out.println("map2:"+getMap("map2-liwx"));
        System.out.println("map2 item3:"+getMapItem("map2-liwx","3"));

        addItem("map2-liwx-s", map2);
        System.out.println(getItem("map2-liwx-s"));
        //System.out.println("map2-s item3"+getMapItem("map2-liwx-s","3"));

        addMapItem("map1-liwx", "5","e");
        System.out.println("map1:"+getMap("map1-liwx"));
        delMapItem("map1-liwx", "1");
        System.out.println("map1:"+getMap("map1-liwx"));


        Set<String> set = new HashSet<String>();
        set.add("aa");
        set.add("bb");
        set.add("cc");
        set.add("dd");
        addSet("set-liwx", set);
        System.out.println("set:"+getSet("set-liwx"));
        Set<String> tree = new TreeSet<String>();
        tree.add("aa");
        tree.add("bb");
        tree.add("cc");
        tree.add("dd");
        addSet("tree-liwx", tree);
        System.out.println("treeset:"+getSet("tree-liwx"));

        List<String> list = new ArrayList<String>();
        list.add("aaaa");
        list.add("bbbb");
        list.add("cccc");
        list.add("dddd");
        delItem("list-liwx");
        addList("list-liwx", list);
        System.out.println("list:"+getList("list-liwx"));
    }
}
