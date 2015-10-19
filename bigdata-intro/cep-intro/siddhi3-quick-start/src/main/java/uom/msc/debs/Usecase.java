package uom.msc.debs;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.ExecutionPlanRuntime;

public abstract class Usecase {
    private List<TestQuery> singleDevicequeries;
    private List<TestQuery> multiDevicequeries;
    protected int execPlanId;
    
    public Usecase(int execPlanId) {
        this.execPlanId = execPlanId;
        singleDevicequeries = new ArrayList<TestQuery>();
        multiDevicequeries = new ArrayList<TestQuery>();
    }
    
    public void addSingleDeviceQuery(TestQuery query) {
        singleDevicequeries.add(query);
    }
    
    public List<TestQuery> getSingleDeviceQueries() {
        return singleDevicequeries;
    }
    
    public void addMultiDeviceQuery(TestQuery query) {
        multiDevicequeries.add(query);
    }
    
    public List<TestQuery> getMultiDeviceQueries() {
        return multiDevicequeries;
    }
    
    public int getExecPlanId() {
        return execPlanId;
    }
    
    public abstract void addCallbacks(ExecutionPlanRuntime executionPlanRuntime);
}
