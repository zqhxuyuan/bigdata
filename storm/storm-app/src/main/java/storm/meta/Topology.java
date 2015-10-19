package storm.meta;

import storm.meta.bolt.MonitorBolt;
import storm.meta.bolt.MysqlBolt;
import storm.meta.bolt.PrintBolt;
import storm.meta.spout.ReadLogSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author blogchong
 * @Blog www.blogchong.com
 * @email blogchong@gmail.com
 * @QQ_G 191321336
 * @version 2014年11月9日 上午11:26:29
 */

/**
 * 主类，只要spout、bolt中有的，可以随意组合top
 */

public class Topology {

	// 实例化TopologyBuilder类。
	private static TopologyBuilder builder = new TopologyBuilder();

	public static void main(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException {
		Config config = new Config();

		// 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
		builder.setSpout("readlog", new ReadLogSpout(), 1);

		// 创建monitor监控过滤节点
		builder.setBolt("monitor", new MonitorBolt("MonitorBolt.xml"), 3)
				.shuffleGrouping("readlog");

		// 创建mysql数据存储节点
		builder.setBolt("mysql", new MysqlBolt("MysqlBolt.xml"), 3)
				.shuffleGrouping("monitor");

		builder.setBolt("print", new PrintBolt(), 3).shuffleGrouping("monitor");

		config.setDebug(false);

		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopology(args[0], config,
					builder.createTopology());
		} else {
			// 这里是本地模式下运行的启动代码。
			config.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("simple", config, builder.createTopology());
		}

	}

}
