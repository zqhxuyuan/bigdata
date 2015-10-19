package storm.starter.model;

import storm.starter.util.WindowConstant;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhengqh on 15/9/21.
 */
public class Event2Tuple {

    public static void main(String[] args) throws Exception{
        long start = System.currentTimeMillis();
        Map<Integer,List<Object>> tupMap = new HashMap<Integer,List<Object>>();
        for(int i=0;i<10000000L;i++){ //1000万个slot, 9s
            tupMap.put(i, null);
        }
        long end = System.currentTimeMillis();
        System.out.println(tupMap.size());
        System.out.println((end - start)/1000);
    }

    public void simple() throws Exception{
        Compute max = Compute.MAX;
        System.out.println(max.name());

        int intMax = Integer.MAX_VALUE;
        System.out.println(intMax);

        for (Compute compute : Compute.values()) {
            System.out.println(compute.name());
        }

        String key = "IP::ACC::1.1.1.1::AA";
        System.out.println(key.substring(0,key.lastIndexOf(WindowConstant.splitKey)));

        System.out.println(1442981735127L/1000);
        System.out.println(1442981735128L/1000);

        DateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = format.parse("20150917");
        //数据不对!
        System.out.println(date.getYear());  //+1900
        System.out.println(date.getMonth()); //+1
        System.out.println(date.getDay());   //??
    }
}
