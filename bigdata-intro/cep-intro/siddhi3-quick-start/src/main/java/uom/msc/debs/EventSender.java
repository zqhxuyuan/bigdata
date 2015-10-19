package uom.msc.debs;

import java.text.DecimalFormat;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class EventSender implements Runnable {
    private int senderId;
    private BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>(2000000);
    private final DescriptiveStatistics qmPublishLatency = new DescriptiveStatistics();
    private InputHandler inputHandler;
    private DecimalFormat f = new DecimalFormat("#.##");
    
    public EventSender(int senderId, InputHandler inputHandler) {
        this.senderId = senderId;
        this.inputHandler = inputHandler;
    }

    public void run() {
        while (true) {
            try {
                Event event = queue.take();
                inputHandler.send(event);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
    }
    
//    public BlockingQueue<Event> getQueue() {
//        return queue;
//    }
    
    public void SendEvent(Event event) throws InterruptedException {
        long clockStart = System.nanoTime();
        
        queue.put(event);

        float timeTookInMs = ((float)(System.nanoTime() - clockStart))/1000000;
        if(timeTookInMs > 0.1){
            qmPublishLatency.addValue(timeTookInMs);
        }
    }
    
    public boolean isQueueEmpty() {
        return queue.isEmpty();
    }
    
    public void printStatistics() {
        System.out.println(new StringBuilder()
        .append("QueuePublishLatency ExecutionPlan=").append(senderId).append(" [")
        .append(UsecaseRunner.testConfigurations)
        .append("|length=").append(qmPublishLatency.getN())
        .append("|Avg=").append(f.format(qmPublishLatency.getMean()))
        .append("|Min=").append(f.format(qmPublishLatency.getMin()))
        .append("|Max=").append(f.format(qmPublishLatency.getMax()))
        .append("|10=").append(f.format(qmPublishLatency.getPercentile(10)))
        .append("|90=").append(f.format(qmPublishLatency.getPercentile(90))).append("]").toString());
        
//        + " Max=" + f.format(qmPublishLatency.getMax())

    }
}
