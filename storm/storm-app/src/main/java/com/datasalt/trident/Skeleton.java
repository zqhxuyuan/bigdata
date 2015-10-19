package com.datasalt.trident;

import java.io.IOException;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * Use this skeleton for starting your own topology that uses the Fake tweets generator as data source.
 * 
 * @author pere
 */
public class Skeleton {

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
		FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", spout)
                .each(new Fields("id", "text", "actor", "location", "date"), new Utils.PrintFilter());

		return topology.build();
	}

    public static StormTopology buildDiffProjectField() throws Exception{
        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

        TridentTopology topology = new TridentTopology();
        //链式调用并不是说第二个依赖第一个的输出.
        //every call to each() allows us to do an implicit projection of the Tuples by selecting a subset of them
        //(the non-projected values will still be available in successive calls 没有被选择的值在后续的调用中仍然是可用的)
        topology.newStream("spout", spout)
                //第一个Filter,选择了id,text字段
                .each(new Fields("id", "text"), new Utils.PrintFilter())
                        //第二个Filter,不是基于第一个的结果,而是和第一个共享同一个输入源
                //因此最终的输出,相同的id,会输出两条记录, 一条是id和text,一条是下面这条
                .each(new Fields("id", "actor", "location", "date"), new Utils.PrintFilter());

        return topology.build();
    }

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		//cluster.submitTopology("hackaton", conf, buildTopology(drpc));
		cluster.submitTopology("hackaton", conf, buildDiffProjectField());
	}
}
