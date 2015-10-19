package storm.starter.test;

import storm.starter.model.Compute;
import storm.starter.model.Dimension;
import storm.starter.model.TDMetric;
import storm.starter.util.WindowConstant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhengqh on 15/9/22.
 */
public class MockMetrics {

    private static Map<String, List<TDMetric>> cache = new HashMap<>();

    private static class SingletonHolder {
        private static final MockMetrics INSTANCE = new MockMetrics();
    }

    //创建单例对象的时候, 模拟数据放到Cache中
    private MockMetrics (){
        cache = mockMetrics();
    }

    public static final MockMetrics getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public static Map<String, List<TDMetric>> getMetricsCache(){
        return getInstance().cache;
    }
    public static List<TDMetric> getMetricsCache(String partner){
        return getInstance().cache.get(partner);
    }

    /*
    public static Map<String, List> getMetricsCache(){
        if(cache!=null && cache.size()>0){
            return cache;
        }else{
            return mockMetrics();
        }
    }
    public static List<TDMetric> getMeticsByPartner(String partner){
        return getMetricsCache().get(partner);
    }

    public static void main(String[] args) {
        Map<String, List> cache = MockMetrics.getMetricsCache();
        System.out.println(cache.size());
        System.out.println("KoudaiMetics:" + cache.get("Koudai").size());

        List<TDMetric> kd = MockMetrics.getMeticsByPartner("Koudai");
        System.out.println(kd.size());
    }
    */

    public static Map<String, List<TDMetric>> mockMetrics(){
        List<String> partners = new ArrayList(){{
            add("Koudai");
            add("Meituan");
            add("Dianping");
            add("Xiechen");
            add("Tongdun");
        }};

        //模拟多个合作方, 多个时间窗口的指标
        for(String partner : partners){
            List<TDMetric> metricList = new ArrayList(){{
                for(Integer time : WindowConstant.timeUnits) {
                    //主维度关联的从维度去重个数
                    add(new TDMetric(partner, partner, "login", Dimension.accountLogin, Dimension.accountEmail, time, Compute.DISTCOUNT));
                    add(new TDMetric(partner, partner, "login", Dimension.accountLogin, Dimension.accountMobile, time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountLogin,Dimension.deviceId,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountLogin,Dimension.ipAddress,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountEmail,Dimension.accountLogin,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountEmail,Dimension.accountMobile,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountEmail,Dimension.deviceId,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountEmail,Dimension.ipAddress,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountMobile,Dimension.accountEmail,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountMobile,Dimension.accountLogin,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountMobile,Dimension.deviceId,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.accountMobile,Dimension.ipAddress,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.ipAddress,Dimension.accountEmail,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.ipAddress,Dimension.accountLogin,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.ipAddress,Dimension.accountMobile,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.ipAddress,Dimension.deviceId,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.deviceId,Dimension.accountEmail,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.deviceId,Dimension.accountLogin,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner,"login", Dimension.deviceId,Dimension.accountMobile,time, Compute.DISTCOUNT));
                    add(new TDMetric(partner,partner, "login", Dimension.deviceId, Dimension.ipAddress, time, Compute.DISTCOUNT));

                    //主维度出现的次数,不需要去重
                    add(new TDMetric(partner, partner, "login", Dimension.accountLogin, null, time, Compute.COUNT));
                    add(new TDMetric(partner, partner, "login", Dimension.accountEmail, null, time, Compute.COUNT));
                    add(new TDMetric(partner, partner, "login", Dimension.accountMobile, null, time, Compute.COUNT));
                    add(new TDMetric(partner, partner, "login", Dimension.ipAddress, null, time, Compute.COUNT));
                    add(new TDMetric(partner, partner, "login", Dimension.deviceId, null, time, Compute.COUNT));
                    add(new TDMetric(partner, partner, "login", Dimension.payAmount, null, time, Compute.COUNT));

                    //求和,最大值,最小值
                    add(new TDMetric(partner, partner, "loan", Dimension.payAmount, null, time, Compute.SUM));
                    add(new TDMetric(partner, partner, "loan", Dimension.payAmount, null, time, Compute.MAX));
                    add(new TDMetric(partner, partner, "loan", Dimension.payAmount, null, time, Compute.MIN));
                }
            }};
            cache.put(partner, metricList);
        }
        return cache;
    }
}
