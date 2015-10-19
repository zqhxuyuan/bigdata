package storm.starter.slidingwindow;

import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WindowedNumCounter {
	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;
	
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;

	public WindowedNumCounter() throws InterruptedException {
		builder = new TopologyBuilder();
		topologyName = "slidingWindowCounts";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(true);
		return conf;
	}

	private void wireTopology() throws InterruptedException {
		String spoutId = "numberGenerator";
		String sumup = "sumup";
		builder.setSpout(spoutId, new NumberSpout(), 2);
		builder.setBolt(sumup, new SumupBolt(6, 2), 1)
				.fieldsGrouping(spoutId, new Fields("number"));
	}

	public void run() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName,
				topologyConfig, runtimeInSeconds);
	}

	public static void main(String[] args) throws Exception {
		new WindowedNumCounter().run();
	}
}
