package storm.starter.length;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;

/**
 * This class implements a simple array store SlidingWindowParticipant.
 * Using an array to store sliding window updates in, we can achieve
 * 'partly parallelized' sliding window, i.e. by telling the SlidingWindowBolt
 * that our sliding window has a length of 3 elements and then telling each of
 * these elements to store e.g. 5 pieces of data. Thus, the sliding window
 * actually has a length of 15, yet consists of only 3 bolts. The method used for
 * distribution of messages that is implemented in SlidingWindowBolt makes sure that
 * the participating bolts will get filled with data and updated uniformly.
 *
 * @author Malte Rohde <malte.rohde@inf.fu-berlin.de>
 */
public abstract class ArraySlidingWindowParticipantBolt extends SlidingWindowParticipantBolt {

    final protected Tuple[] window_data; //保存接收到的数据
    final private int array_size;  // 参与者需要处理的元素数量, 均摊
    private int k;

    /**
     * @param participant_id the ID of this sliding window participant
     * @param array_size the number of messages to store in this bolt
     */
    public ArraySlidingWindowParticipantBolt(int participant_id, int array_size) {
        super(participant_id);

        this.window_data = new Tuple[array_size];
        this.array_size = array_size;
        this.k = 0;
    }

    @Override
    protected void update(Tuple input, BasicOutputCollector collector) {
        //每个参与者接收的数据有多个, 假设20个元素,4个参与者,则每个参与者需要处理5个元素
        //每接收到一个元素,就要调用一次updated. window_data中的数据也采用RoundRobin形式.
        //因为每个参与者最多能处理5个元素,如果第六个元素过来,则最开始的第一个元素会被替换成第六个.
        //这样保证了每个参与者只处理最新的5个元素! 这种方式不需要evict手动失效第一个,只需要替换即可!
        window_data[k++ % array_size] = input;

        updated(collector);
    }

    /**
     * This method is conveniently called _after_ the window_data array
     * has been updated.
     *
     * @param collector
     */
    protected abstract void updated(BasicOutputCollector collector);
}