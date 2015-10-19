package com.zqh.cep.esper.tutorial;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

/**
 * Adding a Listener
 *
 * Listeners are invoked by the engine in response to one or more events that change a statement's result set. Listeners implement the UpdateListener
 * interface and act on EventBean instances as the next code snippet outlines
 *
 * @author doctor
 *
 * @time 2015年5月28日 下午4:02:37
 */
public class MyListener implements UpdateListener {

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        EventBean eventBean = newEvents[0];
        System.out.println("avg = " + eventBean.get("avg(price)"));
    }

}