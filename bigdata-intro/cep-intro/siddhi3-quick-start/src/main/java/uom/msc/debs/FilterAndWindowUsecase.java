package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class FilterAndWindowUsecase extends Usecase {
    private static OutputPerfromanceCalculator performanceCalculator = null;

    public FilterAndWindowUsecase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("matchTimes", "from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)]#window.length(1000) " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addSingleDeviceQuery(new TestQuery("players", "from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100']#window.length(1000) " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addMultiDeviceQuery(new TestQuery("matchTimes", "from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)]#window.length(1000) " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 1));
        
        addMultiDeviceQuery(new TestQuery("players", "from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100']#window.length(1000) " +
                "select sid, ts " +
                "insert into filteredSensorStream;", 0));
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator = new OutputPerfromanceCalculator("filteredSensorStream", 1024);
        
        executionPlanRuntime.addCallback("filteredSensorStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents.length);
//                EventPrinter.print(inEvents);
            }
        });
    }

}
