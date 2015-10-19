package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;

public class MixUsecase extends Usecase {

    private static OutputPerfromanceCalculator performanceCalculator = null;

    public MixUsecase(int execPlanId) {
        super(execPlanId);

        addSingleDeviceQuery(new TestQuery("ballStream", "from sensorStream[(sid == '4' or sid == '8' or sid == '10' or sid == '12') and "
                + "((ts >= 10753295594424116l and ts <= 12557295594424116l) or (ts >= 13086639146403495l and ts <= 14879639146403495l))] "
                + "select sid, ts, x, y "
                + "insert into ballStream;", -1));

        addSingleDeviceQuery(new TestQuery("nearBallStream", "from sensorStream[sid == '4' or sid == '8' or sid == '10' or sid == '12']#window.length(200) as a "
                + "join sensorStream[sid != '4' and sid != '8' and sid != '10' and sid != '12' and sid != '105' and sid != '106']#window.length(200) as b "
                + "on a.x == b.x and a.y == b.y and a.ts > b.ts and (a.ts - b.ts < 1000000000) "
                + "select b.sid as psid, a.sid as bsid, b.ts as pts, a.ts as bts, b.x as px, b.y as py, a.x as bx, a.y as by "
                + "insert into nearBallStream;", 1));

        addSingleDeviceQuery(new TestQuery("ballStream1m", "from sensorStream[(sid == '4' or sid == '8' or sid == '10' or sid == '12') and "
                + "((ts >= 10753295594424116l and ts <= 12557295594424116l) or (ts >= 13086639146403495l and ts <= 14879639146403495l))]#window.length(10000) "
                + "select sid, ts, x, y, avg(v) as avgV "
                + "insert into ballStream1m;", -1));
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator = new OutputPerfromanceCalculator("filteredSensorStream", 1024);

    }

}
