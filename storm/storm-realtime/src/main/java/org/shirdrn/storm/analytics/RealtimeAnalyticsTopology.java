package org.shirdrn.storm.analytics;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.storm.analytics.bolts.EventFilterBolt;
import org.shirdrn.storm.analytics.bolts.EventStatBolt;
import org.shirdrn.storm.analytics.bolts.EventStatResultPersistBolt;
import org.shirdrn.storm.analytics.constants.StatFields;
import org.shirdrn.storm.analytics.utils.RealtimeUtils;
import org.shirdrn.storm.analytics.utils.TestUtils;
import org.shirdrn.storm.commons.constants.Keys;
import org.shirdrn.storm.commons.utils.TopologyUtils;

import storm.kafka.KafkaSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * Real-time event analytics, the topology definition stream graph is depict as follows:
 * <pre>
 * +------------+     +-----------------+     +---------------------+     +----------------------------+
 * | KafkaSpout | --> | EventFilterBolt | --> | EventStatisticsBolt | --> | EventStatResultPersistBolt |
 * +------------+     +-----------------+     +---------------------+     +----------------------------+
 * </pre>
 * <ol>
 * 		<li>{@link KafkaSpout}                 </li> : Read event data from Kafka MQ(topic: topic_json_event) 
 * 		<li>{@link EventFilterBolt}            </li> : Filter and distribute events.
 * 		<li>{@link EventStatBolt}        </li> : Event statistics.
 * 		<li>{@link EventStatResultPersistBolt} </li> : Persist result to Redis.
 * </ol>
 * 
 * @author Yanjun
 */
public class RealtimeAnalyticsTopology {

	private static final Log LOG = LogFactory.getLog(RealtimeAnalyticsTopology.class);
	
	
	private static TopologyBuilder buildTopology(Configuration conf, String topic) {
		return buildTopology(false, conf, topic);
	}
	/**
	 * Configure a topology based on known Storm components(Spout, Bolt).
	 * @param conf
	 * @return
	 */
	private static TopologyBuilder buildTopology(boolean isDebug, Configuration conf, String topic) {
		LOG.info("Building topology...");
		TopologyBuilder builder = new TopologyBuilder();
		// naming Storm components
		String kafkaEventReader = "kafka-event-reader";
		String eventFilter = "event-filter";
		String eventStatistics = "event-statistics";
		String eventStatPersistence = "event-stat-persistence";
		
		// configure Kafka spout
		BaseRichSpout kafkaSpout = null;
		if(isDebug) {
			kafkaSpout = TestUtils.getTestSpout();
		} else {
			kafkaSpout = RealtimeUtils.newKafkaSpout(topic, conf);
		}
		builder.setSpout(kafkaEventReader, kafkaSpout, 1);
		
		// configure filter bolt
		builder
			.setBolt(eventFilter, new EventFilterBolt(), 1)
			.shuffleGrouping(kafkaEventReader)
			.setNumTasks(1);
		
		// configure statistics bolt
		builder
			.setBolt(eventStatistics, new EventStatBolt(), 2)
			.shuffleGrouping(eventFilter)
			.setNumTasks(2);
		
		// configure persistence bolt
		builder
			.setBolt(eventStatPersistence, new EventStatResultPersistBolt(), 2)
			.fieldsGrouping(eventStatistics, new Fields(StatFields.STAT_INDICATOR))
			.setNumTasks(2);
		
		LOG.info("Topology built: " + TopologyUtils.toString(
				RealtimeAnalyticsTopology.class.getSimpleName(), 
				kafkaEventReader, eventFilter, eventStatistics, eventStatPersistence));
		return builder;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration externalConf = RealtimeUtils.getDefaultConfiguration();
		String topic = externalConf.getString(Keys.KAFKA_BROKER_TOPICS);
		
		// submit topology
		String nimbus = externalConf.getString(Keys.STORM_NIMBUS_HOST);
		Config stormConf = new Config();
		String name = RealtimeAnalyticsTopology.class.getSimpleName();
		
		// configure topology
		TopologyBuilder builder = null;
		if(args.length == 0) {
			builder = buildTopology(true, externalConf, topic);
		} else {
			builder = buildTopology(externalConf, topic);
		}
		
		// production use
		if (args != null && args.length > 0) {
			name = args[0];
			stormConf.put(Config.NIMBUS_HOST, nimbus);
			stormConf.setNumWorkers(4);
			StormSubmitter.submitTopologyWithProgressBar(name, stormConf, builder.createTopology());
		} else {
			// debugging using local cluster
			stormConf.setDebug(true);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, stormConf, builder.createTopology());
			int sleep = 60 * 60 * 1000;
			Thread.sleep(sleep);
			cluster.shutdown();
		}
	}
	
}
