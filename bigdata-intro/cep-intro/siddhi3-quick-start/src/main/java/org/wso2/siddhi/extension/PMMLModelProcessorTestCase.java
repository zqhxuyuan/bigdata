package org.wso2.siddhi.extension;

/**
 * Created by zhengqh on 15/10/13.
 */
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class PMMLModelProcessorTestCase {

    private volatile boolean eventArrived;

    @Before
    public void init() {
        eventArrived = false;
    }

    @Test
    public void predictFunctionTest() throws InterruptedException, URISyntaxException {

        URL resource = PMMLModelProcessorTestCase.class.getResource("/decision-tree.pmml");
        String pmmlFile = new File(resource.toURI()).getAbsolutePath();

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "" +
                "define stream InputStream (root_shell double, su_attempted double, num_root double, num_file_creations double, num_shells double, num_access_files double, num_outbound_cmds double, is_host_login double, is_guest_login double, count double, srv_count double, serror_rate double, srv_serror_rate double);";

        //这里并没有把自定义的PMMLModelProcessor注册进来, 为什么可以直接使用#pmml:predict(pmmlFile)??
        //实际上读取的是pmml.siddhiext配置文件: predict=org.wso2.siddhi.extension.PmmlModelProcessor
        //predict已经知道了, 那么前面的pmml是怎么来的: pmml.siddhiext前面的pmml!
        //TODO 文件是怎么作为参数传入的?
        String query = "" +
                "@info(name = 'query1') " +
                "from InputStream#pmml:predict('" + pmmlFile + "') " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    Assert.assertEquals("1", inEvents[0].getData(13));
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 2, 3, 4, 5});
        Thread.sleep(1000);
        junit.framework.Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void predictFunctionWithSelectedAttributesTest() throws InterruptedException, URISyntaxException {

        URL resource = PMMLModelProcessorTestCase.class.getResource("/decision-tree.pmml");
        String pmmlFile = new File(resource.toURI()).getAbsolutePath();

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream (root_shell double, su_attempted double, num_root double, num_file_creations double, num_shells double, num_access_files double, num_outbound_cmds double, is_host_login double, is_guest_login double, count double, srv_count double, serror_rate double, srv_serror_rate double);";

        String query = "" +
                "@info(name = 'query1') " +
                "from InputStream#pmml:predict('" + pmmlFile + "', root_shell, su_attempted, num_root, num_file_creations, num_shells, num_access_files, num_outbound_cmds, is_host_login, is_guest_login, count, srv_count, serror_rate, srv_serror_rate) " +
                "select Predicted_response " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    Assert.assertEquals("1", inEvents[0].getData(0));
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 2, 3, 4, 5});
        Thread.sleep(1000);
        junit.framework.Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}