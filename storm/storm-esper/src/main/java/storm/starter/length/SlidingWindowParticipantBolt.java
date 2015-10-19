package storm.starter.length;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * A simple bolt that should be used to unwrap(打开) the messages sent
 * by SlidingWindowBolt. 接收SlidingWindowBolt发射的消息: 参与者编号,tuple
 *
 * @author Malte Rohde <malte.rohde@inf.fu-berlin.de>
 */
public abstract class SlidingWindowParticipantBolt extends BaseBasicBolt {

    final protected int participant_id;

    /**
     * @param participant_id the ID of this sliding window participant
     */
    public SlidingWindowParticipantBolt(int participant_id) {
        this.participant_id = participant_id;
    }

    @Override
    final public void execute(Tuple input, BasicOutputCollector collector) {
        Integer id = input.getInteger(0);
        Tuple data = (Tuple) input.getValue(1);

        process(data, collector);

        //有多个参与者, 每个参与者都有自己的participant_id, 而input中的id和参与者的编号相等时,
        //说明input的data要给对应的参与者处理, 编号不相等, 则不负责其他参与者的数据.
        if(id == participant_id)
            update(data, collector);
    }

    /**
     * Subclasses must implement this method as a replacement for the
     * Storm <c>execute</c> method.
     *
     * @param input the unwrapped data
     * @param collector the collector
     */
    protected abstract void process(Tuple input, BasicOutputCollector collector);

    /**
     * This method is called when a message containing the same update id as this
     * participant has arrived, yet after the message has been processed. The collector
     * is passed on to this message in case the user wants to emit a message when
     * the sliding window participant updates.
     *
     * @param input
     * @param collector
     */
    protected abstract void update(Tuple input, BasicOutputCollector collector);
}