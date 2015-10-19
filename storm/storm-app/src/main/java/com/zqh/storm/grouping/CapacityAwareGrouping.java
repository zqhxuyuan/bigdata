package com.zqh.storm.grouping;

import backtype.storm.generated.*;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;

import java.util.*;

/**
 * Created by bhargavsarvepalli on 29/06/15.
 * https://github.com/bond1king/StormGrouping
 */
public class CapacityAwareGrouping extends Grouping  implements CustomStreamGrouping {

    private List<Integer> targetTasks;
    private int [] targetStats;
    private TreeSet<Task> capacitySet;
    private Fields fields = null;
    private Fields outFields = null;
    private WorkerTopologyContext context;
    private GlobalStreamId streamId;
    private int msgsPerCapacityUnit = 0;
    private double capacityStreamBolt = 0;
    private double streamlatency = 0;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
        this.context = context;
        if (this.fields != null) {
            this.outFields = context.getComponentOutputFields(stream);
        }
        this.streamId = streamId;
        targetStats = new int[targetTasks.size()];
        capacitySet = new TreeSet<Task>(new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                if (o1.getCapacity() > o2.getCapacity() ) {
                    return -1;
                } else if (o1.getCapacity() < o2.getCapacity() ){
                    return 1;
                } else {
                    return  0;
                }
            }
        });
        for(int i=0; i<targetTasks.size(); i++){
            capacitySet.add(new Task(targetTasks.get(i),0));
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>(1);
        Task t = capacitySet.first();
        targetStats[t.getId()]++;

        if(targetStats[t.getId()] >=  (1- t.getCapacity())*msgsPerCapacityUnit){
            t.setCapacity(1);
            targetStats[t.getId()] = 0;
        }
        boltIds.add(t.getId());
        return boltIds;
    }


    public void summary() {
        TSocket socket = new TSocket("storm1.whizdm.com", 6627);
        TFramedTransport transport = new TFramedTransport(socket);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        Nimbus.Client client = new Nimbus.Client(protocol);
        try {
            transport.open();
            ClusterSummary summary = client.getClusterInfo();
//            String componentId = streamId.get_componentId();
//            Map<String, Map<String, Grouping>> streams = context.getTargets(componentId);
//            Map<String, Grouping> targets = streams.get(streamId.get_streamId());
//

            Iterator<TopologySummary> topologiesIterator =  summary.get_topologies_iterator();
            while (topologiesIterator.hasNext()) {
                TopologySummary topology = topologiesIterator.next();

                if(!topology.get_id().equals(context.getStormId())){
                    continue;
                }

                long boltExecuted = 0;
                double boltLatency = 0;
                double leftOverCapacity = 0;

                TopologyInfo topology_info = client.getTopologyInfo(topology.get_id());
                Iterator<ExecutorSummary> executorStatusItr = topology_info.get_executors_iterator();
                while (executorStatusItr.hasNext()) {
                    ExecutorSummary executor_summary = executorStatusItr.next();
                    ExecutorStats execStats = executor_summary.get_stats();
                    ExecutorSpecificStats execSpecStats = execStats.get_specific();
                    if (execSpecStats.is_set_bolt()) {
                        int taskId = executor_summary.get_executor_info().get_task_start();
                        if(!targetTasks.contains(taskId)){
                            continue;
                        }
                        BoltStats boltStats = execSpecStats.get_bolt();
                        long executed =  get_boltStatLongValueFromMap(boltStats.get_executed(), "600");
                        double latency =  get_boltStatDoubleValueFromMap(boltStats.get_execute_ms_avg(), "600");
                        double capacity = executed * latency / (600000);
                        leftOverCapacity += 1 - capacity;
                        boltExecuted += executed;
                        if(latency > boltLatency){
                            boltLatency = latency;
                        }
                        targetStats[taskId] = 0;
                        capacitySet.add(new Task( taskId ,capacity));
                    }
                }

                msgsPerCapacityUnit = (int)((int)600000/boltLatency/leftOverCapacity);



            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static Long getStatValueFromMap(Map<String, Map<String, Long>> map, String statName) {
        Long statValue = null;
        Map<String, Long> intermediateMap = map.get(statName);
        statValue = intermediateMap.get("default");
        return statValue;
    }

    /*
     * Utility method to parse a Map<> as a special case for Bolts
     */
    public static Double get_boltStatDoubleValueFromMap(Map<String, Map<GlobalStreamId, Double>> map, String statName) {
        Double statValue = 0.0;
        Map<GlobalStreamId, Double> intermediateMap = map.get(statName);
        Set<GlobalStreamId> key = intermediateMap.keySet();
        if(key.size() > 0) {
            Iterator<GlobalStreamId> itr = key.iterator();
            statValue = intermediateMap.get(itr.next());
        }
        return statValue;
    }

    /*
     * Utility method for Bolts
     */
    public static Long get_boltStatLongValueFromMap(Map<String, Map<GlobalStreamId, Long>> map, String statName) {
        Long statValue = null;
        Map<GlobalStreamId, Long> intermediateMap = map.get(statName);
        Set<GlobalStreamId> key = intermediateMap.keySet();
        if(key.size() > 0) {
            Iterator<GlobalStreamId> itr = key.iterator();
            statValue = intermediateMap.get(itr.next());
        }
        return statValue;
    }

    public class Task{
        int id;
        double capacity;

        Task(int id, double capacity){
            this.id = id;
            this.capacity = capacity;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;

        }

        public double getCapacity() {
            return capacity;
        }

        public void setCapacity(double capacity) {
            this.capacity = capacity;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Task task = (Task) o;

            if (id != task.id) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

}

