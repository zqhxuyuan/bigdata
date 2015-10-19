package uom.msc.debs;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class UsecaseRunner {
    private static final Logger log = Logger.getLogger(UsecaseRunner.class);
    private static Options cliOptions;
    public static String testConfigurations;
    private static UsecaseRunner ucRunner;
    
    private SiddhiManager siddhiManager = null;
    private ExecutionPlanRuntime executionPlanRuntimes[] = null;
    private Thread eventSenderThreads[] = null;
    private InputFileReader fileReader = null;
    
    boolean asyncEnabled = true;
    boolean gpuEnabled = false;
    int defaultBufferSize = 1024;
    int threadPoolSize = 4;
    int eventBlockSize = 256;
    boolean softBatchScheduling = true;
    int maxEventBatchSize = 1024;
    int minEventBatchSize = 32;
    int workSize = 0;
    int selectorWorkerCount = 0;
    String inputEventFilePath = null;
    String usecaseName = null;
    String executionPlanName = null;
    int usecaseCountPerExecPlan = 0;
    int execPlanCount = 0;
    boolean multiDevice = false;
    int deviceCount = 0;
    
    static int multiDeviceId = 0;
    
    private static Usecase[] getUsecases(int execPlanId, String usecaseName, int usecaseCountPerExecPlan) {

        Usecase usecases[] = new Usecase[usecaseCountPerExecPlan];
        
        if(usecaseName.compareTo("filter") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new FilterUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("window") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new WindowUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("join") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new JoinUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("filter_window") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new FilterAndWindowUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("filter_join") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new FilterAndJoinUsecase(execPlanId);
            }
        } else if(usecaseName.compareTo("mix") == 0) {
            for(int i=0;i < usecaseCountPerExecPlan; ++i) {
                usecases[i] = new MixUsecase(execPlanId);
            }
        }
        
        return usecases;
    }
    
    private static String getQueryPlan(int executionPlanId, String executionPlanName,
            boolean asyncEnabled, boolean gpuEnabled, 
            int maxEventBatchSize, int minEventBatchSize,
            int eventBlockSize, boolean softBatchScheduling,
            int workSize, int selectorWorkerCount, Usecase usecases[], 
            boolean useMultiDevice, int deviceCount) {
        
        String sensorStream = "@plan:name('" + executionPlanName + executionPlanId + "') " + (asyncEnabled ? "@plan:parallel" : "" ) + " "
                + "define stream sensorStream ( sid string, ts long, "
                + "x int, y int,  z int, "
                + "v double, a double, "
                + "vx int, vy int, vz int, "
                + "ax int, ay int, az int, "
                + "tsr long, tsms long );";
        
        System.out.println("Stream def = [ " + sensorStream + " ]");
        StringBuffer execString = new StringBuffer();
        execString.append(sensorStream);
        
        int usecaseIndex = 0;
        for(Usecase usecase : usecases) {
            List<TestQuery> queries = null;
            if(!useMultiDevice) {
                queries = usecase.getSingleDeviceQueries();
            } else {
                queries = usecase.getMultiDeviceQueries();
            }
            
            for(TestQuery query : queries) {
                StringBuilder sb = new StringBuilder();
                sb.append("@info(name = '" + query.queryId + usecaseIndex + "') ");
                if(gpuEnabled && query.cudaDeviceId >= 0)
                {
                    sb.append("@gpu(");
                    if(!useMultiDevice) {
                        sb.append("cuda.device='").append(query.cudaDeviceId).append("', ");
                    } else {
                        sb.append("cuda.device='").append((multiDeviceId++ % deviceCount)).append("', ");
                    }
                    sb.append("batch.max.size='").append(maxEventBatchSize).append("', ")
                    .append("batch.min.size='").append(minEventBatchSize).append("', ")
                    .append("block.size='").append(eventBlockSize).append("', ")
                    .append("batch.schedule='").append((softBatchScheduling ? "soft" : "hard")).append("', ")
                    .append("string.sizes='symbol=8', ")
                    .append("work.size='").append(workSize).append("', ")
                    .append("selector.workers='").append(selectorWorkerCount).append("' ")
                    .append(") ")
                    .append("@performance(batch.count='1000') ");
                }
                sb.append(query.query);
                String queryString = sb.toString();
                System.out.println(executionPlanName + "::" + query.queryId + " = [ " + queryString + " ]");
                execString.append(queryString);
            }
            usecaseIndex++;
        }
        
        return execString.toString();
    }
    
    public UsecaseRunner() {
        
        siddhiManager = new SiddhiManager();
    }
    
    public void configure(String args[]) {
        CommandLineParser cliParser = new BasicParser();
        CommandLine cmd = null;
        
        try {
            cmd = cliParser.parse(cliOptions, args);
            if (cmd.hasOption("a")) {
                asyncEnabled = Boolean.parseBoolean(cmd.getOptionValue("a"));
            }
            if (cmd.hasOption("g")) {
                gpuEnabled = Boolean.parseBoolean(cmd.getOptionValue("g"));
            }
            if (cmd.hasOption("r")) {
                defaultBufferSize = Integer.parseInt(cmd.getOptionValue("r"));
            }
            if (cmd.hasOption("t")) {
                threadPoolSize = Integer.parseInt(cmd.getOptionValue("t"));
            }
            if (cmd.hasOption("b")) {
                eventBlockSize = Integer.parseInt(cmd.getOptionValue("b"));
            }
            if (cmd.hasOption("Z")) {
                maxEventBatchSize = Integer.parseInt(cmd.getOptionValue("Z"));
            }
            if (cmd.hasOption("z")) {
                minEventBatchSize = Integer.parseInt(cmd.getOptionValue("z"));
            }
            if (cmd.hasOption("s")) {
                softBatchScheduling = !Boolean.parseBoolean(cmd.getOptionValue("s"));
            }
            if (cmd.hasOption("w")) {
                workSize = Integer.parseInt(cmd.getOptionValue("w"));
            }
            if (cmd.hasOption("l")) {
                selectorWorkerCount = Integer.parseInt(cmd.getOptionValue("l"));
            }
            if (cmd.hasOption("m")) {
                multiDevice = Boolean.parseBoolean(cmd.getOptionValue("m"));
                if (cmd.hasOption("d")) {
                    deviceCount = Integer.parseInt(cmd.getOptionValue("d"));
                } else {
                    System.out.println("Please provide device count");
                    Help(); 
                }
            }
            if (cmd.hasOption("i")) {
                inputEventFilePath = cmd.getOptionValue("i");
            }else {
                System.out.println("Please provide input event file path");
                Help();
            }
            if (cmd.hasOption("u")) {
                usecaseName = cmd.getOptionValue("u");
            }else {
                System.out.println("Please provide the usecase name");
                Help();
            }
            if (cmd.hasOption("p")) {
                executionPlanName = cmd.getOptionValue("p");
            }else {
                System.out.println("Please provide the ExecutionPlan name");
                Help();
            }
            if (cmd.hasOption("c")) {
                usecaseCountPerExecPlan = Integer.parseInt(cmd.getOptionValue("c"));
            } else {
                System.out.println("Please provide the usecase count");
                Help();
            }
            if (cmd.hasOption("x")) {
                execPlanCount = Integer.parseInt(cmd.getOptionValue("x"));
            } else {
                System.out.println("Please provide the ExecutionPlan count");
                Help();
            }
            
        } catch (ParseException e) {
            e.printStackTrace();
            Help();
        }
        
        System.out.println("ExecutionPlan : name=" + executionPlanName + " execPalnCount=" + execPlanCount +
                " usecase=" + usecaseName + " usecaseCount=" + usecaseCountPerExecPlan + " useMultiDevice=" + multiDevice);
        System.out.println("Siddhi.Config [EnableAsync=" + asyncEnabled +
                "|GPUEnabled=" + gpuEnabled +
                "|RingBufferSize=" + defaultBufferSize +
                "|ThreadPoolSize=" + threadPoolSize +
                "|EventBlockSize=" + eventBlockSize + 
                "|EventBatchMaxSize=" + maxEventBatchSize +
                "|EventBatchMinSize=" + minEventBatchSize +
                "|SoftBatchScheduling=" + softBatchScheduling + 
                "]");
        
        String mode = (gpuEnabled ? (multiDevice ? "gpu_md" : "gpu_sd") : (asyncEnabled ? "cpu_mt" : "cpu_st"));
        testConfigurations = "xpc=" + execPlanCount + "|uc=" + usecaseCountPerExecPlan  
                + "|mode=" + mode + "|rb=" + defaultBufferSize + "|bs=" + eventBlockSize + "|bmin=" + minEventBatchSize
                + "|bmax=" + maxEventBatchSize; 

//        siddhiManager.getSiddhiContext().setEventBufferSize(defaultBufferSize);
//        siddhiManager.getSiddhiContext().setThreadPoolInitSize(threadPoolSize);
//        siddhiManager.getSiddhiContext().setExecutorService(new ThreadPoolExecutor(threadPoolSize, Integer.MAX_VALUE,
//                60L, TimeUnit.SECONDS,
//                new LinkedBlockingDeque<Runnable>()));
//                Executors.new newFixedThreadPool(threadPoolSize);
//        siddhiManager.getSiddhiContext().setScheduledExecutorService(Executors.newScheduledThreadPool(threadPoolSize));
        
        executionPlanRuntimes = new ExecutionPlanRuntime[execPlanCount];
        eventSenderThreads = new Thread[execPlanCount];
        
        fileReader = new InputFileReader(inputEventFilePath, this);
        
        for(int i=0; i<execPlanCount; ++i) {
            Usecase usecases[] = getUsecases(i, usecaseName, usecaseCountPerExecPlan);
            String queryPlan = getQueryPlan(i, executionPlanName, asyncEnabled, gpuEnabled, maxEventBatchSize, 
                    minEventBatchSize, eventBlockSize, softBatchScheduling, workSize, selectorWorkerCount, usecases, 
                    multiDevice, deviceCount);
             
            executionPlanRuntimes[i] = siddhiManager.createExecutionPlanRuntime(queryPlan);
            
            for(Usecase usecase: usecases) {
                usecase.addCallbacks(executionPlanRuntimes[i]);
            }
            
            InputHandler inputHandlerSensorStream = executionPlanRuntimes[i].getInputHandler("sensorStream");
            executionPlanRuntimes[i].start();
            
            EventSender sensorEventSender = new EventSender(i, inputHandlerSensorStream);
            eventSenderThreads[i] = new Thread(sensorEventSender);
            
//            fileReader.addQueue(sensorEventSender.getQueue());
            fileReader.addEventSender(sensorEventSender);
        }
    }
    
    public void start() throws InterruptedException {
        for(Thread t: eventSenderThreads) {
            t.start();
        }
        
        Thread fileReaderThread = new Thread(fileReader);
        fileReaderThread.start();
        
        for(Thread t: eventSenderThreads) {
            t.join();
        }
        fileReaderThread.join();
    }
    
    public void onEnd() {
        System.out.println("ExecutionPlan : name=" + executionPlanName + " OnEnd");
        
        List<SummaryStatistics> statList  = new ArrayList<SummaryStatistics>();
        for(ExecutionPlanRuntime execplan : executionPlanRuntimes) {
            //execplan.getStatistics(statList);
            execplan.enableStatistics();
        }
        
//        DescriptiveStatistics totalStatistics = new DescriptiveStatistics();
//        SummaryStatistics totalStatistics = new SummaryStatistics();
//        for(DescriptiveStatistics stat : statList) {
//            for(Double d : stat.getValues()) {
//                totalStatistics.addValue(d); 
//            }
//        }
        StatisticalSummaryValues totalStatistics = AggregateSummaryStatistics.aggregate(statList);
        
        for(ExecutionPlanRuntime execplan : executionPlanRuntimes) {
            execplan.shutdown();
        }
        
        final DecimalFormat decimalFormat = new DecimalFormat("###.##"); 
        
        System.out.println(new StringBuilder()
        .append("EventProcessTroughputEPS ExecutionPlan=").append(executionPlanName)
        .append("|").append(UsecaseRunner.testConfigurations)
        .append("|DatasetCount=").append(statList.size())
        .append("|length=").append(totalStatistics.getN())
        .append("|Avg=").append(decimalFormat.format(totalStatistics.getMean()))
        .append("|Min=").append(decimalFormat.format(totalStatistics.getMin()))
        .append("|Max=").append(decimalFormat.format(totalStatistics.getMax()))
        .append("|Var=").append(decimalFormat.format(totalStatistics.getVariance()))
        .append("|StdDev=").append(decimalFormat.format(totalStatistics.getStandardDeviation())).toString());
//        .append("|10=").append(decimalFormat.format(totalStatistics.getPercentile(10)))
//        .append("|90=").append(decimalFormat.format(totalStatistics.getPercentile(90))).toString());
    }
    
    private static void Help() {
        // This prints out some help
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("UsecaseRunner", cliOptions);
        System.exit(0);
    }
    
    @SuppressWarnings("restriction")
    public static void main(String [] args) throws InterruptedException {
        cliOptions = new Options();
        cliOptions.addOption("a", "enable-async", true, "Enable Async processing");
        cliOptions.addOption("g", "enable-gpu", true, "Enable GPU processing");
        cliOptions.addOption("r", "ringbuffer-size", true, "Disruptor RingBuffer size - in power of two");
        cliOptions.addOption("Z", "batch-max-size", true, "GPU Event batch max size");
        cliOptions.addOption("z", "batch-min-size", true, "GPU Event batch min size");
        cliOptions.addOption("t", "threadpool-size", true, "Executor service pool size");
        cliOptions.addOption("b", "events-per-tblock", true, "Number of Events per thread block in GPU");
        cliOptions.addOption("s", "strict-batch-scheduling", true, "Strict batch size policy");
        cliOptions.addOption("w", "work-size", true, "Number of events processed by each GPU thread - 0=default");
        cliOptions.addOption("i", "input-file", true, "Input events file path");
        cliOptions.addOption("u", "usecase", true, "Name of the usecase");
        cliOptions.addOption("p", "execplan", true, "Name of the ExecutionPlan");
        cliOptions.addOption("c", "usecase-count", true, "Usecase count per ExecutionPlan");
        cliOptions.addOption("x", "execplan-count", true, "ExecutionPlan count");
        cliOptions.addOption("m", "use-multidevice", true, "Use multiple GPU devices");
        cliOptions.addOption("d", "device-count", true, "GPU devices count");
        cliOptions.addOption("l", "selector-workers", true, "Number of worker thread for selector processor - 0=default");
        
        ucRunner = new UsecaseRunner();
        ucRunner.configure(args);
        ucRunner.start();
        
        Signal.handle(new Signal("INT"), new SignalHandler() {
            public void handle(Signal sig) {
                ucRunner.onEnd();
                System.exit(-1);
            }
        });
        
        Signal.handle(new Signal("KILL"), new SignalHandler() {
            public void handle(Signal sig) {
                ucRunner.onEnd();
                System.exit(-1);
            }
        });
        
        System.exit(0);
    }
}
