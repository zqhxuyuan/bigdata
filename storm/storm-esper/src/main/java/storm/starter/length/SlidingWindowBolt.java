package storm.starter.length;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A simple bolt that envelopes(信封) each incoming message in another
 * message and adds an 'update-sliding-window' field. This field
 * contains the index of the array element to be replaced by the
 * current message, as if the sliding window was implemented as
 * a ring buffer backed by an array.
 *
 * @author Malte Rohde <malte.rohde@inf.fu-berlin.de>
 *     https://github.com/maltoe/storm-slidingwindow
 */
public class SlidingWindowBolt extends BaseBasicBolt {

    final private int size;
    private int k;

    /**
     * @param size length of the sliding window (in messages)
     */
    public SlidingWindowBolt(int size) {
        this.size = size;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //这个Bolt接收上游Spout发射的数据,它的功能是: 将接收的数据发射给哪一个参与者.
        //每当接收到一个Tuple,就会调用一次该execute方法. 而k是Bolt全局的,
        //所以每接收一个tuple,k++. k可以用来表示这个Bolt目前接收到的tuple数量
        //这种算法类似于RoundRobin,即第一个tuple给第一个参与者,第二个tuple给第二个参与者....
        int participant = k++ % size;
        //input属于/会被哪一个participant处理. 注意这里加上参与者编号后,直接转发完整的Tuple input.
        collector.emit(new Values(new Integer(participant), input));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("update-sliding-window", "data"));
    }
}