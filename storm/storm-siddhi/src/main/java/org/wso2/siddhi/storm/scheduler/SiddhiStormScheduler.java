package org.wso2.siddhi.storm.scheduler;

import backtype.storm.scheduler.*;
import clojure.lang.MapEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;


public class SiddhiStormScheduler implements IScheduler{

    private class SupervisorAllocations{
        private SupervisorDetails supervisor;
        private Map<String, Integer> allocatedComponents = new HashMap<String, Integer>();

        public SupervisorAllocations(SupervisorDetails supervisor){
            this.supervisor = supervisor;
        }

        public void addAllocation(String componentId, int count){
            allocatedComponents.put(componentId, count);
        }

        public Map<String, Integer> getAllocations(){
            return this.allocatedComponents;
        }
    }

    private static final String COMPONENT_DELIMITER = ",";
    private static final String VALUE_DELIMITER = ":";
    public static final String SIDDHI_TOPOLOGY_NAME = "LatencyMeasureTopology";
    private static Log log = LogFactory.getLog(SiddhiStormScheduler.class);
    Map<Integer, SupervisorAllocations> supervisorAllocationsMap = new HashMap<Integer, SupervisorAllocations>();
    Map conf;

    public void prepare(Map conf) {
        this.conf = conf;
    }

    // Decoding elements in CompID1:1,CompID2:*,CompID3:2 format
    private void indexAllocations(Collection<SupervisorDetails> supervisorDetails){
        for (SupervisorDetails supervisor : supervisorDetails) {
            Map meta = (Map) supervisor.getSchedulerMeta();
            String allocationConfig = (String) meta.get("dedicated.to.component");

            if (allocationConfig != null && !allocationConfig.isEmpty()){
                int allocationOrdering = (meta.get("allocation.ordering") == null) ? Integer.MAX_VALUE :
                        Integer.parseInt((String) meta.get("allocation.ordering"));

                String[] allocations = allocationConfig.split(COMPONENT_DELIMITER);
                SupervisorAllocations supervisorAllocations = new SupervisorAllocations(supervisor);

                for (int i = 0; i < allocations.length; i++){
                    String allocation = allocations[i].trim();
                    String[] values = allocation.split(VALUE_DELIMITER);
                    supervisorAllocations.addAllocation(values[0].trim(), Integer.parseInt(values[1].trim()));
                    supervisorAllocationsMap.put(allocationOrdering, supervisorAllocations);
                    // Putting into a hash map to enforce allocation ordering by it's automatic sorting
                }
            }
        }
    }

    private void freeAllSlots(SupervisorDetails supervisor, Cluster cluster){
        for (Integer port : cluster.getUsedPorts(supervisor)) {
            cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
        }
    }

    public void schedule(Topologies topologies, Cluster cluster) {
        log.info("Scheduling by Siddhi Storm Scheduler");
        TopologyDetails siddhiTopology = topologies.getByName(SIDDHI_TOPOLOGY_NAME);

        if (siddhiTopology != null){
            boolean isSchedulingNeeded = cluster.needsScheduling(siddhiTopology);

            if (!isSchedulingNeeded){
                log.info(SIDDHI_TOPOLOGY_NAME + " already scheduled!");
            }else{
                log.info("Scheduling "+ SIDDHI_TOPOLOGY_NAME);
                indexAllocations(cluster.getSupervisors().values());
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(siddhiTopology);

                for (Map.Entry<Integer, SupervisorAllocations> entry : supervisorAllocationsMap.entrySet()){
                    SupervisorAllocations supervisorAllocations = entry.getValue();

                    //TODO : Create lists for each slot of the supervisor
                    for (Map.Entry<String, Integer> allocationEntry : supervisorAllocations.getAllocations().entrySet()){
                        // TODO : Collect executors of all componenets for the supervisor into an array
                    }
                }
                /*
                for(Map.Entry<String, List<ExecutorDetails>> entry : componentToExecutors.entrySet()){
                    String componentName = entry.getKey();
                    List<ExecutorDetails> executors = entry.getValue();
                    SupervisorDetails supervisor = componentToSupervisor.get(componentName);

                    if (supervisor != null){
                        log.info("Scheduling " + componentName + " on " + supervisor.getHost());
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);

                        if (availableSlots.isEmpty()){
                            throw new RuntimeException("No free slots available for scheduling in dedicated supervisor @ " + supervisor.getHost());
                        }

                        int availableSlotCount = availableSlots.size();
                        int executorCount = executors.size();

                        List<List<ExecutorDetails>> executorsForSlots = new ArrayList<List<ExecutorDetails>>();
                        for (int i = 0; i < availableSlotCount; i++){
                            executorsForSlots.add(new ArrayList<ExecutorDetails>());
                        }

                        for (int i = 0; i < executorCount; i++){
                            int slotToAllocate = i % availableSlotCount;
                            ExecutorDetails executor = executors.get(i);
                            executorsForSlots.get(slotToAllocate).add(executor);
                        }

                        for (int i = 0; i < availableSlotCount; i++){
                            if (!executorsForSlots.get(i).isEmpty()){
                                cluster.assign(availableSlots.get(i), siddhiTopology.getId(), executorsForSlots.get(i));
                            }
                        }
                    }else{
                        log.info("No dedicated supervisor for " + componentName);
                    }
                }
                */
            }
        }
        new EvenScheduler().schedule(topologies, cluster);
    }
}




