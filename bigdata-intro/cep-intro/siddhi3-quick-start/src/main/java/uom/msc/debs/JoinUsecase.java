package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class JoinUsecase extends Usecase {
    private static OutputPerfromanceCalculator performanceCalculator1 = null;
    private static OutputPerfromanceCalculator performanceCalculator2 = null;
    private static OutputPerfromanceCalculator performanceCalculator3 = null;
    
    public JoinUsecase(int execPlanId) {
        super(execPlanId);
        
        addSingleDeviceQuery(new TestQuery("nearBallStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12']#window.length(1000) as a "
                + "join sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106']#window.length(1000) as b "
                + "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) " 
                + "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by " 
                + "insert into nearBallStream;", 1));

        
//        addSingleDeviceQuery(new TestQuery("ballStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12'] "
//                + "select sid, ts, x, y "
//                + "insert into ballStream;", -1));
//        
//        addSingleDeviceQuery(new TestQuery("playersStream", "from sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106'] "
//                + "select sid, ts, x, y "
//                + "insert into playersStream;", -1));
//
//        addSingleDeviceQuery(new TestQuery("nearBall", "from ballStream#window.length(2000) as a " +
//                "join playersStream#window.length(200) as b " +
//                "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) " +
//                "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by " +
//                "insert into nearBallStream;", 1));
        
//        addMultiDeviceQuery(new TestQuery("ballStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12'] "
//                + "select sid, ts, x, y "
//                + "insert into ballStream;", 1));
//        
//        addMultiDeviceQuery(new TestQuery("playersStream", "from sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106'] "
//                + "select sid, ts, x, y "
//                + "insert into playersStream;", 1));
//
//        addMultiDeviceQuery(new TestQuery("nearBall", "from ballStream#window.length(2000) as a " +
//                "join playersStream#window.length(200) as b " +
//                "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) " +
//                "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by " +
//                "insert into nearBallStream;", 1));
        
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator1 = new OutputPerfromanceCalculator("nearBallStream", 1000000);
//        performanceCalculator2 = new OutputPerfromanceCalculator("ballStream", 1024);
//        performanceCalculator3 = new OutputPerfromanceCalculator("playersStream", 1024);
        
        executionPlanRuntime.addCallback("nearBallStream", new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                performanceCalculator1.calculate(inEvents.length);
//                System.out.print("nearBallStream : ");
//                EventPrinter.print(inEvents);
            }
        });
        
//        executionPlanRuntime.addCallback("ballStream", new StreamCallback() {
//            @Override
//            public void receive(Event[] inEvents) {
//                performanceCalculator2.calculate(inEvents.length);
////                System.out.print("ballStream : ");
////                EventPrinter.print(inEvents);
//            }
//        });
//        
//        executionPlanRuntime.addCallback("playersStream", new StreamCallback() {
//            @Override
//            public void receive(Event[] inEvents) {
//                performanceCalculator3.calculate(inEvents.length);
////                System.out.print("playersStream : ");
////                EventPrinter.print(inEvents);
//            }
//        });
    }

}
