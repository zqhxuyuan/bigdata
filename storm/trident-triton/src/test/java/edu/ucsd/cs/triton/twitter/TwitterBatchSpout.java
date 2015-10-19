package edu.ucsd.cs.triton.twitter;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


public class TwitterBatchSpout implements IBatchSpout, StatusListener
{
    private static final long serialVersionUID = 1L;

    private transient BlockingQueue<Status> queue;
    private transient TwitterStream twitterStream;

    @Override
    public void onException(Exception ex)
    {
    }

    @Override
    public void onStatus(Status status)
    {
        queue.offer(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
    {
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses)
    {
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId)
    {
    }

		@Override
    public void onStallWarning(StallWarning arg0) {
	    // TODO Auto-generated method stub
	    
    }

		@Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
	    // TODO Auto-generated method stub
      this.queue = new ArrayBlockingQueue<Status>(1000);

      //Configuration twitterConf = new ConfigurationBuilder().setUser(username).setPassword(pwd).build();
      TwitterStreamFactory fact = new TwitterStreamFactory();

      twitterStream = fact.getInstance();
      twitterStream.addListener(this);
      twitterStream.sample();
    }

		@Override
    public void emitBatch(long batchId, TridentCollector collector) {
	    // TODO Auto-generated method stub
      Status value = queue.poll();
      if (value == null) {
          Utils.sleep(50);
      }
      else {
          collector.emit(tuple(value.getCreatedAt(), value.getRetweetCount()));            
      }
    }

		@Override
    public void ack(long batchId) {
	    // TODO Auto-generated method stub
	    
    }

    @Override
    public void close()
    {
        twitterStream.shutdown();
    }
    
		@SuppressWarnings("rawtypes")
    @Override
    public Map getComponentConfiguration() {
	    // TODO Auto-generated method stub
	    return null;
    }

		@Override
    public Fields getOutputFields() {
	    // TODO Auto-generated method stub
	    return new Fields("createdAt", "retweetCount");

    }
}
