package storm.meta.helloworld;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author blogchong
 * @Blog www.blogchong.com
 * @email blogchong@gmail.com
 * @QQ_G 191321336
 * @version 2014年11月9日 下午21:50:29
 */

// 这是一个简单实例，统计词频wordcoutn。storm中的helloworld
public class HelloWorldTopology {

	// 实例化TopologyBuilder类。
	private static TopologyBuilder builder = new TopologyBuilder();

	public static void main(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		Config config = new Config();

		// 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
		builder.setSpout("Random", new ReadFileSpout(), 1);

		// 标准化输入
		builder.setBolt("Norm", new WordNormalizerBolt(), 2).shuffleGrouping(
				"Random");

		// 单词计数
		builder.setBolt("Count", new WordCountBolt(), 3).fieldsGrouping("Norm",
				new Fields("word"));

		builder.setBolt("print", new PrintWorldCountBolt(), 1).shuffleGrouping(
				"Count");

		config.setDebug(false);

		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopology(args[0], config,
					builder.createTopology());
		} else {
			// 这里是本地模式下运行的启动代码。
			config.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("helloword", config,
					builder.createTopology());
		}

	}
}
