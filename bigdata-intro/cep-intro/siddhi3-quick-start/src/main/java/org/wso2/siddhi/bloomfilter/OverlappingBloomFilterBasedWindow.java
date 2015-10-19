package org.wso2.siddhi.bloomfilter;

/**
 * Created by zhengqh on 15/10/13.
 */
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.util.bloom.BloomFilter;
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
 * Overlapping Bloom Filter class joins an incoming event with an existing event
 * in the Siddhi window by comparing the join attribute. Before going through
 * the window it checks with Bloom Filters whether the join attribute exists and
 * only if it is true, it proceeds with Queue search.
 */
public class OverlappingBloomFilterBasedWindow {//implements SiddhiWindow {

    private int expectedNoOfEventsPerWindow;
    private int[] joinAttributeId;
    private int overLapPrecentage;
    private ArrayList<BloomFilter> filterList;
    private ArrayList<Integer> noOfEventList;
    private BlockingQueue<StreamEvent> eventQueue;

    private int bloomFilterSize;
    private int nonOverLapEvents;
    private int noOfHash;
    private int oldestAddingFilter;
    private int newestAddingFilter;
    private int removeEventCount;

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
     * @param expressions
     *            [4] overlap percentage - Overlap percentage of the filter
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
            overLapPrecentage = 0;
        } else if (expressions.length == 4) {
            expectedNoOfEventsPerWindow = ((IntConstant) expressions[0]).getValue();
            joinAttributeId[2] = ((IntConstant) expressions[1]).getValue();
            joinAttributeId[3] = ((IntConstant) expressions[2]).getValue();
            bloomFilterSize = ((IntConstant) expressions[3]).getValue();
            overLapPrecentage = 0;
        } else if (expressions.length == 5) {
            expectedNoOfEventsPerWindow = ((IntConstant) expressions[0]).getValue();
            joinAttributeId[2] = ((IntConstant) expressions[1]).getValue();
            joinAttributeId[3] = ((IntConstant) expressions[2]).getValue();
            bloomFilterSize = ((IntConstant) expressions[3]).getValue();
            overLapPrecentage = ((IntConstant) expressions[4]).getValue();
        } else {
            throw new OperationNotSupportedException(
                    "Parameters count is not matching. There should be three, four or five  parameters ");
        }

        if (bloomFilterSize < BloomFilterUtil.optimalBloomFilterSize(expectedNoOfEventsPerWindow,
                0.001) ||
                bloomFilterSize > BloomFilterUtil.optimalBloomFilterSize(expectedNoOfEventsPerWindow,
                        0.00001)) {
            throw new OperationNotSupportedException("Bloomfilter size not suitable");
        }

        nonOverLapEvents =
                expectedNoOfEventsPerWindow -
                        (int) (overLapPrecentage / 100.0 * expectedNoOfEventsPerWindow);

        noOfHash = BloomFilterUtil.optimalNoOfHash(bloomFilterSize, expectedNoOfEventsPerWindow);

        oldestAddingFilter = 0;
        newestAddingFilter = 0;

        filterList = new ArrayList<BloomFilter>();
        filterList.add(new BloomFilter(bloomFilterSize, noOfHash, Hash.MURMUR_HASH));

        noOfEventList = new ArrayList<Integer>();
        noOfEventList.add(0);

    }

    /**
     * Pass stream events to the addStreamEvent method by checking the event
     * type. State events are also converted into stream events.
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
            removLastEventFromFilters();
            return eventQueue.poll();
        } else {
            return eventQueue.peek();
        }
    }

    /**
     * Search value is initially checked with the filters and if it exists,
     * search through the queue for a matching event. Outputs a matched event
     * list.
     *
     * @param attributeName
     *            Search attribute
     * @param value
     *            Search value
     * @return foundEventList
     *            Matched event list
     */
    public List<ComplexEvent> find(String attributeName, Object value) {

        Key key = new Key(value.toString().getBytes());
        List<ComplexEvent> foundEventList = new ArrayList<ComplexEvent>();

        boolean isInFilters = false;

        for (int i = 0; i <= oldestAddingFilter; i++) {
            if (filterList.get(i).membershipTest(key)) {
                isInFilters = true;
                break;
            }
        }

        if (isInFilters) {
            Iterator<StreamEvent> iterator = eventQueue.iterator();
            while (iterator.hasNext()) {
                StreamEvent streamEvent = iterator.next();
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
     *            State or stream event
     */
    public void remove(ComplexEvent event) throws OperationNotSupportedException {
        throw new OperationNotSupportedException("Operation not supported with bloomfilters");
    }

    /**
     * Return current states of window and filter
     */
    public Object[] currentState() {
        return new Object[] { expectedNoOfEventsPerWindow, bloomFilterSize, joinAttributeId,
                overLapPrecentage, filterList, noOfEventList, eventQueue,
                nonOverLapEvents, noOfHash, oldestAddingFilter, newestAddingFilter,
                removeEventCount };
    }

    /**
     * Restore window,filters and variables
     *
     * @param objects
     *            Object array with current states
     */
    public void restoreState(Object[] objects) {

        expectedNoOfEventsPerWindow = (Integer) objects[0];
        bloomFilterSize = (Integer) objects[1];
        joinAttributeId = (int[]) objects[2];
        overLapPrecentage = (Integer) objects[3];

        filterList = (ArrayList<BloomFilter>) objects[4];
        noOfEventList = (ArrayList<Integer>) objects[5];
        eventQueue = (BlockingQueue<StreamEvent>) objects[6];

        nonOverLapEvents = (Integer) objects[7];
        noOfHash = (Integer) objects[8];
        oldestAddingFilter = (Integer) objects[9];
        newestAddingFilter = (Integer) objects[10];
        removeEventCount = (Integer) objects[11];

    }

    /**
     * Returns the current number of events in the window
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
     * Add stream events to the eventQueue and filters
     *
     * @param streamEvent
     *            Stream type event to add
     */

    private void addStreamEvent(StreamEvent streamEvent) {

        eventQueue.add(streamEvent);

        Key key = new Key((streamEvent.getAttribute(joinAttributeId)).toString().getBytes());

        for (int i = oldestAddingFilter; i <= newestAddingFilter; i++) {
            filterList.get(i).add(key);
            noOfEventList.set(i, noOfEventList.get(i) + 1);
        }

        if (noOfEventList.get(newestAddingFilter) % nonOverLapEvents == 0) {
            filterList.add(new BloomFilter(bloomFilterSize, noOfHash, Hash.MURMUR_HASH));
            noOfEventList.add(0);
            newestAddingFilter++;
        }

        if (noOfEventList.get(oldestAddingFilter) == expectedNoOfEventsPerWindow) {
            oldestAddingFilter++;
        }

    }

    /**
     * "Remove event count" will be increased even though the removal is not
     * happened actually. When it reaches the non overlapping events, oldest
     * bloom filter is removed since all the current events resides in the other
     * bloom filters.
     */
    private void removLastEventFromFilters() {

        if (removeEventCount == nonOverLapEvents) {
            filterList.remove(0);
            noOfEventList.remove(0);
            newestAddingFilter--;
            oldestAddingFilter--;
            removeEventCount = 0;
        }

        removeEventCount++;

    }

}