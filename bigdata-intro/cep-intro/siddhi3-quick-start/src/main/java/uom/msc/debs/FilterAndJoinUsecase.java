package uom.msc.debs;

import org.wso2.siddhi.core.ExecutionPlanRuntime;

public class FilterAndJoinUsecase extends Usecase {
    private static OutputPerfromanceCalculator performanceCalculator = null;

    public FilterAndJoinUsecase(int execPlanId) {
        super(execPlanId);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void addCallbacks(ExecutionPlanRuntime executionPlanRuntime) {
        performanceCalculator = new OutputPerfromanceCalculator("filteredSensorStream", 1024);

    }

}
