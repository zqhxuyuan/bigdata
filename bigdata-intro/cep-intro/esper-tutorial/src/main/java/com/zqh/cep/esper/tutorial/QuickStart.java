package com.zqh.cep.esper.tutorial;

import com.espertech.esper.client.*;

import java.util.Random;

/**
 * @see http://www.espertech.com/esper/quickstart.php
 * @author doctor
 * @time 2015年5月28日 下午3:51:18
 */
public class QuickStart {

    public static void main(String[] args) {

        // Configuration
        //
        // Esper runs out of the box and no configuration is required. However configuration can help make statements more readable and provides the
        // opportunity to plug-in extensions and to configure relational database access.
        //
        // One useful configuration item specifies Java package names from which to take event classes.
        //
        // This snippet of using the configuration API makes the Java package of the OrderEvent class known to an engine instance:
        // In order to query the OrderEvent events, we can now remove the package name from the statement:see line40

        Configuration configuration = new Configuration();
        configuration.addEventTypeAutoName("com.zqh.cep.esper.tutorial");

        // Creating a Statement
        // A statement is a continuous query registered with an Esper engine instance that provides results to listeners as new data arrives, in
        // real-time, or by demand via the iterator (pull) API.
        // The next code snippet obtains an engine instance and registers a continuous query. The query returns the average price over all OrderEvent
        // events that arrived in the last 30 seconds:
        EPServiceProvider epServiceProvider = EPServiceProviderManager.getDefaultProvider(configuration);
        String expression = "select avg(price) from OrderEvent.win:time(60 sec)";
        EPStatement epStatement = epServiceProvider.getEPAdministrator().createEPL(expression);

        // By attaching the listener to the statement the engine provides the statement's results to the listener:
        MyListener myListener = new MyListener();
        epStatement.addListener(myListener);

        // Sending events
        // The runtime API accepts events for processing. As a statement's results change, the engine indicates the new results to listeners right
        // when the events are processed by the engine.
        // Sending events is straightforward as well:
        EPRuntime runtime = epServiceProvider.getEPRuntime();

        OrderEvent orderEvent = new OrderEvent("shirt", 75.50D);
        runtime.sendEvent(orderEvent);

        for(int i=0;i<100000;i++){
            OrderEvent event = new OrderEvent("shirt", new Random().nextInt(100));
            runtime.sendEvent(event);
        }
    }

}
