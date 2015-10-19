package uom.msc.debs;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class OutputPerfromanceCalculator {
    String name;
    int count = 0;
    int eventCount = 0;
    int batchCount = 0;
    volatile long start = System.currentTimeMillis();
//    private final DescriptiveStatistics statistics = new DescriptiveStatistics();
    final DecimalFormat decimalFormat = new DecimalFormat("###.##");
    
    public OutputPerfromanceCalculator(String name, int batchCount) {
        this.name = "<" + name + ">";
        this.batchCount = batchCount;
    }
    
    public void calculate(int currentEventCount) {
        eventCount += currentEventCount;
        count++;
        if (count % batchCount == 0) {
            long end = System.currentTimeMillis();
            double tp = ((eventCount) * 1000.0) / (end - start);
            System.out.println(name + " Throughput = " + decimalFormat.format(tp) + " Event/sec " + (eventCount));
            start = end;
            eventCount = 0;
        }
    }

}
