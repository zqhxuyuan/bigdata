package org.wso2.siddhi.bloomfilter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;

public class CountingBloomFilterBasedWindowTest {

    private CountingBloomFilterBasedWindow window;
    private int count;
    int windowSize;

    @Before
    public void setUp() {

        window = new CountingBloomFilterBasedWindow();
        windowSize = 10;
        Expression[] expressions =
                { new IntConstant(windowSize), new IntConstant(1),
                        new IntConstant(0) };
        count = 0;
        window.init(expressions);
    }

    @Test
    public void testWithStreamEvent() {

        addStreamEvent(createStreamEvent("Hp", 210.45, 23));
        addStreamEvent(createStreamEvent("Apple", 3500.50, 10));
        addStreamEvent(createStreamEvent("Dell", 245.95, 78));

        Assert.assertTrue(window.find("price", 1000.55).isEmpty());
        Assert.assertFalse(window.find("price", 3500.50).isEmpty());

        addStreamEvent(createStreamEvent("Toshiba", 234.45, 24));
        addStreamEvent(createStreamEvent("Dell", 456.45, 80));
        addStreamEvent(createStreamEvent("Hp", 875.98, 70));
        addStreamEvent(createStreamEvent("Hp", 223.87, 190));
        addStreamEvent(createStreamEvent("Apple", 356.97, 76));
        addStreamEvent(createStreamEvent("Hp", 678.87, 65));
        addStreamEvent(createStreamEvent("Toshiba", 235.95, 19));
        addStreamEvent(createStreamEvent("Dell", 879.25, 57));
        addStreamEvent(createStreamEvent("Hp", 345.45, 19));
        addStreamEvent(createStreamEvent("Hp", 879.76, 86));
        addStreamEvent(createStreamEvent("Apple", 719.82, 29));
        addStreamEvent(createStreamEvent("Hp", 985.30, 112));

        Assert.assertTrue(window.find("price", 3500.50).isEmpty());

        addStreamEvent(createStreamEvent("Toshiba", 234.45, 24));
        addStreamEvent(createStreamEvent("Dell", 456.45, 80));
        addStreamEvent(createStreamEvent("Hp", 875.98, 70));
        addStreamEvent(createStreamEvent("Hp", 223.87, 190));
        addStreamEvent(createStreamEvent("Apple", 356.97, 76));
        addStreamEvent(createStreamEvent("Hp", 678.87, 65));
        addStreamEvent(createStreamEvent("Toshiba", 235.95, 19));
        addStreamEvent(createStreamEvent("Dell", 879.25, 57));
        addStreamEvent(createStreamEvent("Hp", 345.45, 19));
        addStreamEvent(createStreamEvent("Hp", 879.76, 86));
        addStreamEvent(createStreamEvent("Apple", 719.82, 29));
        addStreamEvent(createStreamEvent("Hp", 985.30, 112));

    }

    @Test
    public void testWithStateEvent() {

        StreamEvent[] streamEvents1 =
                { createStreamEvent("Hp", 210.45, 23),
                        createStreamEvent("Apple", 3500.50, 10),
                        createStreamEvent("Dell", 245.95, 78) };

        //TODO 3.0.0-snapshot
        //addStateEvent(new StateEvent(streamEvents1), 3);

        Assert.assertTrue(window.find("price", 1000.55).isEmpty());
        Assert.assertFalse(window.find("price", 3500.50).isEmpty());

        StreamEvent[] streamEvents2 =
                { createStreamEvent("Toshiba", 234.45, 24),
                        createStreamEvent("Dell", 456.45, 80),
                        createStreamEvent("Hp", 875.98, 70),
                        createStreamEvent("Hp", 223.87, 190),
                        createStreamEvent("Apple", 356.97, 76) };

        //addStateEvent(new StateEvent(streamEvents2), 5);
        Assert.assertFalse(window.find("price", 3500.50).isEmpty());

        StreamEvent[] streamEvents3 =
                { createStreamEvent("Hp", 678.87, 65),
                        createStreamEvent("Toshiba", 235.95, 19),
                        createStreamEvent("Dell", 879.25, 57),
                        createStreamEvent("Hp", 345.45, 19),
                        createStreamEvent("Hp", 879.76, 86),
                        createStreamEvent("Apple", 719.82, 29),
                        createStreamEvent("Hp", 985.30, 112) };

        //addStateEvent(new StateEvent(streamEvents3), 7);

        Assert.assertTrue(window.find("price", 3500.50).isEmpty());

    }

    @Test
    public void testGetLast() {

        StreamEvent event = createStreamEvent("Dell", 245.95, 78);
        addStreamEvent(event);

        addStreamEvent(createStreamEvent("Hp", 210.45, 23));
        addStreamEvent(createStreamEvent("Apple", 3500.50, 10));

        Assert.assertEquals(event, (StreamEvent) window.getLast(false));
        Assert.assertEquals(event, (StreamEvent) window.getLast(true));
        Assert.assertNotSame(event, (StreamEvent) window.getLast(false));

    }

    private StreamEvent createStreamEvent(String symbol, double price, int quantity) {
        return this.createStreamEvent(symbol, price, quantity, System.currentTimeMillis());
    }

    private StreamEvent createStreamEvent(String symbol, double price, int quantity, long timeStamp) {
        StreamEvent streamEvent = new StreamEvent(0, 1, 2);
        streamEvent.setOnAfterWindowData(price, 0);
        streamEvent.setOutputData(symbol, 0);
        streamEvent.setOutputData(quantity, 1);
        streamEvent.setTimestamp(timeStamp);
        return streamEvent;
    }

    private void addStreamEvent(StreamEvent event) {
        if (count == windowSize) {
            window.getLast(true);
            count--;
        }
        window.add(event);
        count++;
    }

    private void addStateEvent(StateEvent event, int streamEventCount) {
        while (windowSize < count + streamEventCount) {
            window.getLast(true);
            count--;
        }
        window.add(event);
        count += streamEventCount;
    }
}