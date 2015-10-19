package com.neulion.stream;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.neulion.stream.bolt.OneHourOnlineVisitorsBolt;
import com.neulion.stream.bolt.TenSecondOnlineVisitorsBolt;
import com.neulion.stream.spout.OneHourLogSpout;
import com.neulion.stream.spout.TenSecondLogSpout;

public class EsperTopologyTest {

	public static void main(String[] argv) {
        oneHour();
    }

    /**
     * online (visitors: 532, views: 533)
     * appType (desktop:287,unknown:142,android_phone:60,ipad:39,android_tablet:2,iphone:2)
     * deviceType (unknown:287,iPad:171,GT-I9100_samsung:8,GT-I9300_samsung:5,GT-N7100_samsung:5,iPhone:5,GT-P5110_samsung:4,Nexus 7_asus:3,GT-N7105_samsung:3,iPodtouch:3,S100_unknown:2,GT-N8010_samsung:2,ASUS Transformer Pad TF300T_asus:1,SCH-I535_samsung:1,GT-I9100T_samsung:1,HTC Desire C_HTC:1,SHV-E160S_samsung:1,GT-N7105T_samsung:1,GT-P7510_samsung:1,PMP5080CPRO_unknown:1,IM-A800S_PANTECH:1,A501_Acer:1,GT-S5300_samsung:1,GT-P3100_samsung:1,GT-I9300T_samsung:1,HTC Desire HD A9191_HTC:1,SHV-E210K_samsung:1,GT-P3113_samsung:1,Transformer Prime TF201_asus:1,GT-N7000_samsung:1,SC-05D_samsung:1,HTC One X_HTC:1,HTC One S_HTC:1,SHW-M440S_samsung:1,AMOI N820_AMOI:1,Nexus 4_LGE:1,LG-SU870_LGE:1,SO-04D_Sony:1,GT-I9210_samsung:1,GT-I9305_samsung:1,GT-P5100_samsung:1,SHW-M480W_samsung:1,GT-N8000_samsung:1,HTC Vision_HTC:1,HTC Desire_HTC:1,XT910_motorola:1)
     */
    public static void oneHour(){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("onehour", new OneHourLogSpout(), 1);
        builder.setBolt("print", new OneHourOnlineVisitorsBolt(), 1).shuffleGrouping("onehour");

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }

    public static void tenSecond(){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tensecond", new TenSecondLogSpout(), 1);
        builder.setBolt("print", new TenSecondOnlineVisitorsBolt(), 1).shuffleGrouping("tensecond");

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
    }
}
