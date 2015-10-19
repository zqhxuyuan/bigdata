package com.github.shuliga;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.github.shuliga.bolt.command.CommandEventRoutingBolt;
import com.github.shuliga.bolt.command.NotificationBolt;
import com.github.shuliga.bolt.stream.JsonWriterBolt;
import com.github.shuliga.bolt.stream.PharmacyFraudDetectorBolt;
import com.github.shuliga.bolt.stream.PharmacyFraudIndexBolt;
import com.github.shuliga.context.JMSContext;
import com.github.shuliga.context.JMSContextProducer;
import com.github.shuliga.spout.CommandEventSpout;
import com.github.shuliga.spout.PharmacyDataSpout;
import com.github.shuliga.utils.FileUtil;

import javax.jms.JMSException;
import java.util.logging.Logger;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class PharmacyAbuseDetection {

	private static final int DEFAULT_RUNTIME_IN_SECONDS = 3600;
	private static final int TOP_N = 5;
	public static final int SPOUT_PARALLELISM_HINT = 10;
	private static final Double FRAUD_THRESHOLD = 1.0;

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	public PharmacyAbuseDetection() throws InterruptedException {
		builder = new TopologyBuilder();
		topologyName = "com.github.shuliga.PharmacyAbuseDetection";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		initContext();
		wireEventApiTopology();
		wirePharmacyStreamTopology();
	}

	private void initContext() {
		new JMSContextProducer().fillContext(JMSContext.INSTANCE);
		try {
			JMSContext.INSTANCE.connection.start();
		} catch (JMSException e) {
			Logger.getLogger(getClass().getName(), e.getMessage());
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setNumWorkers(1);
		conf.setMaxSpoutPending(100);
		return conf;
	}

	private void wirePharmacyStreamTopology() throws InterruptedException {
		// Pharmacy data abuse detection topology
		String spoutId = "pharmacyData";
		String fraudIndexerId = "fraudIndexer";
		String fraudDetectorId = "fraudDetector";
		String jsonWriterId = "jsonWriter";
		builder.setSpout(spoutId, new PharmacyDataSpout(), SPOUT_PARALLELISM_HINT);
		builder.setBolt(fraudIndexerId, new PharmacyFraudIndexBolt(), 4).shuffleGrouping(spoutId);
		builder.setBolt(fraudDetectorId, new PharmacyFraudDetectorBolt(FRAUD_THRESHOLD)).globalGrouping(fraudIndexerId);
		builder.setBolt(jsonWriterId, new JsonWriterBolt(FileUtil.PERSISTENCE_ROOT)).globalGrouping(fraudDetectorId);
	}

	private void wireEventApiTopology() throws InterruptedException {
		// com.github.shuliga.EventAPI commands routing and notification topology
		String cmdSpoutId = "commandEvent";
		String commandRouterId = "commandRouter";
		String notificationBoltId = "notificationBolt";
		builder.setSpout(cmdSpoutId, new CommandEventSpout(), 1);
		builder.setBolt(commandRouterId, new CommandEventRoutingBolt(), 1).shuffleGrouping(cmdSpoutId);
		builder.setBolt(notificationBoltId, new NotificationBolt(), 1).globalGrouping(commandRouterId);
	}

	public void run() throws InterruptedException {
		try {
			StormSubmitter.submitTopology(topologyName, topologyConfig, builder.createTopology());
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		new PharmacyAbuseDetection().run();
	}

}
