package storm.starter.slidingwindow;

import backtype.storm.Config;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.starter.test.PrintBolt;
import storm.starter.test.TestTupleSpout;
import storm.starter.util.StormRunner;

public class WindowedTopologyDistinctCount {
	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	private static final int DEFAULT_RUNTIME_IN_SECONDS = 300;

	public WindowedTopologyDistinctCount() throws InterruptedException {
		builder = new TopologyBuilder();
		topologyName = "slidingWindowCounts";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(false);
		return conf;
	}

	private void wireTopology() throws InterruptedException {
		String spoutId = "numberGenerator";
		String count = "count";
		builder.setSpout(spoutId, new TestTupleSpout("left","right"), 2);
		builder.setBolt(count, new DistinctCountBolt(40, 10), 1)
				.fieldsGrouping(spoutId, new Fields("left"));
        builder.setBolt("print", new PrintBolt()).shuffleGrouping(count);
	}

	public void run() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName,
				topologyConfig, runtimeInSeconds);
	}

	public static void main(String[] args) throws Exception {
		new WindowedTopologyDistinctCount().run();
	}
}
