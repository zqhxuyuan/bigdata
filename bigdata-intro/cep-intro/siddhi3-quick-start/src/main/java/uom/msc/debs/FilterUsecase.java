package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class FilterUsecase extends Usecase {

    private static OutputPerfromanceCalculator performanceCalculator = null;
    
    public FilterUsecase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("ballStream", "from sensorStream[(sid == '4' or sid == '8' or sid == '10' or sid == '12') and "
        		+ "((ts >= 10753295594424116l and ts <= 12557295594424116l) or (ts >= 13086639146403495l and ts <= 14879639146403495l))] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", 1));
        
//        addSingleDeviceQuery(new TestQuery("playersStream", "from sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106'] "
//                + "select sid, ts, x, y "
//                + "insert into playersStream;", 1));
        
//        addSingleDeviceQuery(new TestQuery("matchTimes", "from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
//                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
//                "select sid, ts " +
//                "insert into filteredSensorStream;", 1));
//        
//        addSingleDeviceQuery(new TestQuery("players", "from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
//                "select sid, ts " +
//                "insert into filteredSensorStream;", 1));
//        
//        addMultiDeviceQuery(new TestQuery("matchTimes", "from sensorStream[(ts >= 10753295594424116l and ts <= 12557295594424116l) or "
//                + "(ts >= 13086639146403495l and ts <= 14879639146403495l)] " +
//                "select sid, ts " +
//                "insert into filteredSensorStream;", 1));
//        
//        addMultiDeviceQuery(new TestQuery("players", "from sensorStream[sid != '97' and sid != '98' and sid != '99' and sid != '100'] " +
//                "select sid, ts " +
//                "insert into filteredSensorStream;", 1));
        
    }
    
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator = new OutputPerfromanceCalculator("ballStream", 1024);
        
        executionPlanRuntime.addCallback("ballStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator.calculate(inEvents.length);
//                EventPrinter.print(inEvents);
            }
        });
        
//        executionPlanRuntime.addCallback("sensorStream", new StreamCallback() {
//            @Override
//            public void receive(Event[] inEvents) {
//                EventPrinter.print(inEvents);
//            }
//        });
        
    }
}
