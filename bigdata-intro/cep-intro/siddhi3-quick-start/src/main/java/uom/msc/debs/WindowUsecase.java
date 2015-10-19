package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class WindowUsecase extends Usecase {

    private static OutputPerfromanceCalculator performanceCalculator = null;
    
    public WindowUsecase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("matchTimes", "from sensorStream#window.length(50000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 1));
        
        addSingleDeviceQuery(new TestQuery("players", "from sensorStream#window.length(50000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 1));
        
        addMultiDeviceQuery(new TestQuery("matchTimes", "from sensorStream#window.length(1000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 1));
        
        addMultiDeviceQuery(new TestQuery("players", "from sensorStream#window.length(10000) " +
                "select sid, ts, x, y " +
                "insert into windowSensorStream;", 1));
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator = new OutputPerfromanceCalculator("windowSensorStream", 1024000);
        
        executionPlanRuntime.addCallback("windowSensorStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents.length);
//                EventPrinter.print(inEvents);
            }
        });
    }

}
