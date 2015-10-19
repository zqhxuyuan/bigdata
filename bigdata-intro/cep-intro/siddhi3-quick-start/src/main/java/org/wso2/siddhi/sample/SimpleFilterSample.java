/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.sample;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 *
 * Events{ @timeStamp = 1444903849056, inEvents = [Event{timestamp=1444903849056, data=[IBM, 700.0], isExpired=false}], RemoveEvents = null }
 * Events{ @timeStamp = 1444903849057, inEvents = [Event{timestamp=1444903849057, data=[GOOG, 50.0], isExpired=false},
 *                                                 Event{timestamp=1444903849057, data=[WSO2, 45.6], isExpired=false}], RemoveEvents = null }
 * 为什么后面的两个事件是放在一起的?
 */
public class SimpleFilterSample {

    public static void main(String[] args) throws InterruptedException {

        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //执行计划: 首先定义流的数据结构,类似定义表结构
        //然后查询这张表, 计算(过滤), 输出到另一个流
        String executionPlan = "" +
                "define stream cseEventStream (symbol string, price float, volume long); " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[volume < 150] " +
                "select symbol,price " +
                "insert into outputStream ;";

        //Generating runtime
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        //Adding callback to retrieve output events from query
        //回调函数: 从查询中接收输出事件
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            /**
             * timestamp: 查询发生的时间
             * inEvents: 接收的事件(输入进来的事件)
             * removeEvents: 被移除的事件(过期的才会被移除,一般用在窗口函数中)
             *
             * 每当发送一条事件到Siddhi中.
             */
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });

        //Retrieving InputHandler to push events into Siddhi 接收处理器, 用于将事件push到Siddhi中
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");

        //Starting event processing
        executionPlanRuntime.start();

        //Sending events to Siddhi 发送事件给Siddhi
        inputHandler.send(new Object[]{"IBM", 700f, 100l});   //+
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
        inputHandler.send(new Object[]{"GOOG", 50f, 30l});    //+
        inputHandler.send(new Object[]{"IBM", 76.6f, 400l});
        inputHandler.send(new Object[]{"WSO2", 45.6f, 50l});  //+
        Thread.sleep(500);

        //Shutting down the runtime
        executionPlanRuntime.shutdown();

        //Shutting down Siddhi
        siddhiManager.shutdown();

    }
}
