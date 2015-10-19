package org.wso2.siddhi.bloomfilter;

/**
 * Created by zhengqh on 15/10/13.
 */
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
//import org.wso2.siddhi.core.util.collection.SiddhiWindow;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;

/**
 * Counting Bloom Filter class joins an incoming event with an existing event in
 * the Siddhi window by comparing the join attribute. Before going through the
 * window it checks with the Bloom Filter whether the join attribute exists and
 * only if it is true, it proceeds with window search.
 */
public class CountingBloomFilterBasedWindow {//implements SiddhiWindow {

    private int expectedNoOfEventsPerWindow;
    private int[] joinAttributeId;
    private CountingBloomFilter filter;
    private int bloomFilterSize;

    private BlockingQueue<StreamEvent> eventQueue;

    /**
     * Bloom filters will be created with the given expectedNoOfEventsPerWindow
     * (windowSize). Initialize the join attribute according to the expression.
     *
     * @param expressions
     *            [0] expected No Of Events in the window
     * @param expressions
     *            [1] if value is 0 - beforeWindowData if value is 1 -
     *            onOrbeforeWindowData if value is 2 - outputDataWindowData
     * @param expressions
     *            [2] value is the array position of the above three arrays
     * @param expressions
     *            [3] bloomFilterSize - Size of the bloom filter
     */
    public void init(Expression[] expressions) throws OperationNotSupportedException {

        eventQueue = new LinkedBlockingQueue<StreamEvent>();

        joinAttributeId = new int[4];
        joinAttributeId[0] = 0;
        joinAttributeId[1] = 0;

        if (expressions.length == 3) {
            expectedNoOfEventsPerWindow = ((IntConstant) expressions[0]).getValue();
            joinAttributeId[2] = ((IntConstant) expressions[1]).getValue();
            joinAttributeId[3] = ((IntConstant) expressions[2]).getValue();
            bloomFilterSize =
                    BloomFilterUtil.optimalBloomFilterSize(expectedNoOfEventsPerWindow,
                            0.0005);
        } else if (expressions.length == 4) {
            expectedNoOfEventsPerWindow = ((IntConstant) expressions[0]).getValue();
            joinAttributeId[2] = ((IntConstant) expressions[1]).getValue();
            joinAttributeId[3] = ((IntConstant) expressions[2]).getValue();
            bloomFilterSize = ((IntConstant) expressions[3]).getValue();
        } else {
            throw new OperationNotSupportedException(
                    "Parameters count is not matching, There should be three or four parameters ");
        }

        if (bloomFilterSize < BloomFilterUtil.optimalBloomFilterSize(expectedNoOfEventsPerWindow,
                0.001) ||
                bloomFilterSize > BloomFilterUtil.optimalBloomFilterSize(expectedNoOfEventsPerWindow,
                        0.00001)) {
            throw new OperationNotSupportedException("Bloomfilter size not suitable");
        }

        int noOfHash =
                BloomFilterUtil.optimalNoOfHash(bloomFilterSize, expectedNoOfEventsPerWindow);

        filter = new CountingBloomFilter(bloomFilterSize, noOfHash, Hash.MURMUR_HASH);

    }

    /**
     * Pass stream events to the add method by checking the event type. State
     * events are also converted into stream events.
     *
     * @param event
     *            State or stream event
     */
    public void add(ComplexEvent event) {

        if (event.getClass() == StreamEvent.class) {
            addStreamEvent((StreamEvent) event);
        } else if (event.getClass() == StateEvent.class) {
            StateEvent stateEvent = (StateEvent) event;
            // TODO Due to the limitations of State event class
            int position = 0;
            while (true) {
                try {
                    //addStreamEvent(stateEvent.getEvent(position));
                    addStreamEvent(stateEvent.getStreamEvent(position));
                    position++;
                } catch (ArrayIndexOutOfBoundsException ex) {
                    break;
                }
            }
        }

    }

    /**
     * Remove and returns or retrieve the last element without removing
     *
     * @param remove
     *            To remove or not
     */
    public ComplexEvent getLast(Boolean remove) {
        if (remove) {
            StreamEvent streamEvent = eventQueue.poll();
            filter.delete(new Key((byte[]) streamEvent.getAttribute(joinAttributeId).toString()
                    .getBytes()));
            return streamEvent;
        } else {
            return eventQueue.peek();
        }
    }

    /**
     * Search value is initially checked with the filter and if it exists,
     * search through the queue for a matching event. Outputs a matched event
     * list.
     *
     * @param attributeName
     *            Search Attribute
     * @param value
     *            Search value
     * @return  foundEventList
     *            Matched event list
     */
    public List<ComplexEvent> find(String attributeName, Object value) {

        List<ComplexEvent> foundEventList = new ArrayList<ComplexEvent>();

        Key key = new Key(value.toString().getBytes());

        if (filter.membershipTest(key)) {
            Iterator<StreamEvent> iterator = eventQueue.iterator();

            while (iterator.hasNext()) {
                StreamEvent streamEvent = (StreamEvent) iterator.next();
                if (streamEvent.getAttribute(joinAttributeId).equals(value)) {
                    foundEventList.add(streamEvent);
                }
            }
        }
        return foundEventList;
    }

    /**
     * This Operation not supported
     *
     * @param event
     *
     */
    public void remove(ComplexEvent event) throws OperationNotSupportedException {
        throw new OperationNotSupportedException("Operation not supported with bloomfilters");
    }

    /**
     * Return current states of window and filter
     */
    public Object[] currentState() {

        return new Object[] { eventQueue, filter, expectedNoOfEventsPerWindow, joinAttributeId,
                bloomFilterSize };

    }

    /**
     * Restore window,filters and variables
     *
     * @param objects
     *            Object array with current states
     */
    public void restoreState(Object[] objects) {

        eventQueue = (BlockingQueue<StreamEvent>) objects[0];
        filter = (CountingBloomFilter) objects[1];
        expectedNoOfEventsPerWindow = (Integer) objects[2];
        joinAttributeId = (int[]) objects[3];
        bloomFilterSize = (Integer) objects[4];

    }

    /**
     * Returns the current number of events in the queue
     */
    public int size() {
        return eventQueue.size();
    }

    /**
     * Free up any used resources
     */
    public void destroy() {

    }

    /**
     * Add stream events to the eventQueue and filter
     *
     * @param streamEvent
     *            Stream type event to add
     */
    private void addStreamEvent(StreamEvent streamEvent) {
        eventQueue.add(streamEvent);
        Key key = new Key((streamEvent.getAttribute(joinAttributeId)).toString().getBytes());
        filter.add(key);
    }

}