package edu.ucsd.cs.triton.builtin.spout;

import static backtype.storm.utils.Utils.tuple;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class TwitterSpout extends BaseRichSpout implements StatusListener
{
    private static final long serialVersionUID = 1L;

    private transient BlockingQueue<Status> queue;
    private transient SpoutOutputCollector collector;
    private transient TwitterStream twitterStream;

    @Override
    public void ack(Object arg0)
    {
    }

    @Override
    public void fail(Object arg0)
    {
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf,
                     TopologyContext context,
                     SpoutOutputCollector collector)
    {
        this.queue = new ArrayBlockingQueue<Status>(1000);
        this.collector = collector;

        TwitterStreamFactory fact = new TwitterStreamFactory();

        twitterStream = fact.getInstance();
        twitterStream.addListener(this);
        twitterStream.sample();
    }

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
    public void close()
    {
        twitterStream.shutdown();
    }

    @Override
    public void nextTuple()
    {
        Status value = queue.poll();
        if (value == null) {
            Utils.sleep(50);
        }
        else {
            collector.emit(tuple(value.getCreatedAt(), value.getRetweetCount(), value.getUser()));            
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("createdAt", "retweetCount", "user"));
    }


		@Override
    public void onStallWarning(StallWarning arg0) {
	    // TODO Auto-generated method stub
	    
    }

		public Fields getOutputFields() {
	    // TODO Auto-generated method stub
	    return null;
    }
}
