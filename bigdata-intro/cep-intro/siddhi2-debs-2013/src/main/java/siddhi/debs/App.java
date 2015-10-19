package siddhi.debs;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.tracer.EventMonitor;
import org.wso2.siddhi.core.tracer.LogEventMonitor;
import org.wso2.siddhi.core.tracer.PassThroughEventMonitor;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

public class App {
    static final Logger m_Log = Logger.getLogger(App.class);
    private DataFeeder m_DataFeeder;
    private SiddhiManager m_SiddhiManager;
    private SiddhiConfiguration m_Configuration;
    private InputHandler m_InputHandler;
    private EventMonitor m_EventMonitor;

    private long l_ResultCounter;
    private boolean b_FirstOutput;
    private long m_StartTime;
    private long m_EndTime;

    public App(DataFeeder _mFeeder) {
        m_DataFeeder = _mFeeder;
        l_ResultCounter = 0;
        b_FirstOutput = true;

        SetupStream();
        SetupQueries();
    }

    //two streams: Players – events from players and BallStream – events from the ball.
    //An event consists of the sensorID, timestamp, x,y,z coordinates, velocity and acceleration vectors.
    private String gameStream = "define stream GameStream (" +
            "sid int, " +
            "timestamp long, " +
            "pos_x int, " +
            "pos_y int, " +
            "pos_z int, " +
            "velocity int, " +
            "accel int, " +
            "velo_x int, " +
            "velo_y int, " +
            "velo_z int, " +
            "accel_x int, " +
            "accel_y int, " +
            "accel_z int" +
            ")";

    private void SetupStream() {
        Boolean bAsyncProcessing = Boolean.parseBoolean(System.getProperty("siddhi.AsyncProcessing", "0"));
        int iThreadSchedulerCorePoolSize = Integer.parseInt(System.getProperty("siddhi.ThreadSchedulerCorePoolSize", "2"));
        int iThreadExecutorCorePoolSize = Integer.parseInt(System.getProperty("siddhi.ThreadExecutorCorePoolSize", "2"));
        int iThreadExecutorMaxPoolSize = Integer.parseInt(System.getProperty("siddhi.ThreadExecutorMaxPoolSize", "2"));
        Boolean bEnableStats = Boolean.parseBoolean(System.getProperty("siddhi.EnableStats", "0"));
        Boolean bEnableTrace = Boolean.parseBoolean(System.getProperty("siddhi.EnableTrace", "0"));
        String sEventMonitor = System.getProperty("siddhi.EventMonitor", "PassThroughEventMonitor");

        System.out.println("siddhi.AsyncProcessing             : " + bAsyncProcessing);
        System.out.println("siddhi.ThreadSchedulerCorePoolSize : " + iThreadSchedulerCorePoolSize);
        System.out.println("siddhi.ThreadExecutorCorePoolSize  : " + iThreadExecutorCorePoolSize);
        System.out.println("siddhi.ThreadExecutorMaxPoolSize   : " + iThreadExecutorMaxPoolSize);
        System.out.println("siddhi.EnableStats                 : " + bEnableStats);
        System.out.println("siddhi.EnableTrace                 : " + bEnableTrace);
        System.out.println("siddhi.EventMonitor                : " + sEventMonitor);

        m_Configuration = new SiddhiConfiguration();
        m_Configuration.setAsyncProcessing(bAsyncProcessing);
        m_Configuration.setThreadSchedulerCorePoolSize(iThreadSchedulerCorePoolSize);
        m_Configuration.setThreadExecutorCorePoolSize(iThreadExecutorCorePoolSize);
        m_Configuration.setThreadExecutorMaxPoolSize(iThreadExecutorMaxPoolSize);

        //扩展类
        try {
            List<Class> lstExtClasses = new ArrayList<Class>();

            Class<?> extClass = Class.forName("siddhi.debs.TwoDimDistanceCalculatorExtension");
            lstExtClasses.add(extClass);

            extClass = Class.forName("siddhi.debs.SequenceCountCalculatorExtension");
            lstExtClasses.add(extClass);

            m_Configuration.setSiddhiExtensions(lstExtClasses);
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }

        m_SiddhiManager = new SiddhiManager(m_Configuration);
        m_SiddhiManager.enableStats(bEnableStats);
        m_SiddhiManager.enableTrace(bEnableTrace);

        if (sEventMonitor == "PassThrewEventMonitor") {
            m_EventMonitor = new PassThroughEventMonitor();
        } else {
            m_EventMonitor = new LogEventMonitor();
        }
        m_SiddhiManager.setEventMonitor(m_EventMonitor);

        //定义流的数据结构
        try {
            m_InputHandler = m_SiddhiManager.defineStream(gameStream);

        } catch (SiddhiParserException ex) {
            ex.printStackTrace();
        }

        //设置输入数据的处理器
        m_DataFeeder.SetInputHandler(m_InputHandler);

    }

    private void SetupQueries() {
//		SetupFilterQuery();
//		SetupJoinQuery();
        SetupSequenceQuery();
    }

    private void SetupJoinQuery() {
        try {
            m_SiddhiManager.addQuery("from GameStream [sid == 4 or sid == 8 or sid == 10 or sid == 12] " +
                    "select sid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Ball;");

//			m_SiddhiManager.addQuery("from GameStream [sid != 4 and sid != 8 and sid != 10 and sid != 12 and sid != 105 and sid != 106] " +
//        			"select sid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
//        			"insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 13 or sid == 14 or sid == 97 or sid == 98] " +
                    "select sid, 'Nick Gertje' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 47 or sid == 16] " +
                    "select sid, 'Dennis Dotterweich' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 49 or sid == 88] " +
                    "select sid, 'Niklas Waelzlein' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 19 or sid == 52] " +
                    "select sid, 'Wili Sommer' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 53 or sid == 54] " +
                    "select sid, 'Philipp Harlass' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 23 or sid == 24] " +
                    "select sid, 'Roman Hartleb' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 57 or sid == 58] " +
                    "select sid, 'Erik Engelhardt' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 59 or sid == 28] " +
                    "select sid, 'Sandro Schneider' as pid, 'A' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 61 or sid == 62 or sid == 99 or sid == 100] " +
                    "select sid, 'Leon Krapf' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 63 or sid == 64] " +
                    "select sid, 'Kevin Baer' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 65 or sid == 66] " +
                    "select sid, 'Luca Ziegler' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 67 or sid == 68] " +
                    "select sid, 'Ben Mueller' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 69 or sid == 38] " +
                    "select sid, 'Vale Reitstetter' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 71 or sid == 40] " +
                    "select sid, 'Christopher Lee' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 73 or sid == 74] " +
                    "select sid, 'Leon Heinze' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

            m_SiddhiManager.addQuery("from GameStream [sid == 75 or sid == 44] " +
                    "select sid, 'Leo Langhans' as pid, 'B' as tid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
                    "insert into Players;");

//			m_SiddhiManager.addQuery("from GameStream [sid == 105 or sid == 106] " +
//        			"select sid, timestamp, pos_x, pos_y, pos_z, velocity/100000 as vel, accel/1000000 as acc " +
//        			"insert into Referee;");

            m_SiddhiManager.addQuery("from Ball#window.length(1) as b join " +
                    "Players#window.length(1) as p unidirectional " +
                    "on convert(debs:getTwoDimDistance(b.pos_x, b.pos_y, p.pos_x, p.pos_y),double) < 1000 and convert(b.acc,double) > 55 " +
                    "select p.sid as playerSid, p.pid as playerId, p.tid as teamId, p.timestamp, p.pos_x, p.pos_y, p.pos_z, b.acc, b.sid as ballSid " +
                    "insert into HitStream;");

            m_SiddhiManager.addQuery("from h1 = HitStream, " +
                    "b1 = Ball[h1.ballSid == sid and pos_x < 52483 and pos_x > 0 and pos_y > -33960 and pos_y < 33965]*, " +
                    "b2 = Ball[h1.ballSid == sid and (pos_x > 52483 or pos_x < 0 or pos_y < -33960 or pos_y > 33965)] " +
                    "select b2.sid as ballSid, b2.timestamp, b2.pos_x, b2.pos_y, b2.pos_z " +
                    "insert into BallLeaveStream;");

            m_SiddhiManager.addQuery("from old = HitStream, " +
                    "b = HitStream[old.playerId != playerId], " +
                    "n = HitStream[b.playerId == playerId]*, " +
                    "e1 = HitStream[b.playerId != playerId] or e2 = BallLeaveStream " +
                    "select b.playerId, b.teamId, b.timestamp as startTs, coalesce(e1.timestamp ,e2.timestamp) as endTs " +
                    "insert into BallPossessionStream;");


        } catch (SiddhiParserException ex) {
            ex.printStackTrace();
        }

        StreamCallback mCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                m_Log.info("Result Event : " + Arrays.deepToString(events));

                l_ResultCounter += events.length;
                if (b_FirstOutput) {
                    b_FirstOutput = false;
                    m_StartTime = System.currentTimeMillis();
                } else {
                    m_EndTime = System.currentTimeMillis();
                }
            }
        };

//        m_SiddhiManager.addCallback("Ball", mCallback);
//        m_SiddhiManager.addCallback("Players", mCallback);
        m_SiddhiManager.addCallback("HitStream", mCallback);
        m_SiddhiManager.addCallback("BallLeaveStream", mCallback);
        m_SiddhiManager.addCallback("BallPossessionStream", mCallback);
    }

    private void SetupSequenceQuery() {
        try {
            m_SiddhiManager.definePartition("define partition Player by GameStream.sid");

            m_SiddhiManager.addQuery("from s = GameStream [velocity > 1000000] , " +
                    "     t = GameStream [velocity >= 0 and velocity <= 1000000]+ , " +
                    "     e = GameStream [(velocity > 1000000) and ((timestamp - s.timestamp) > 1000000000000L)] " +
                    "select s.timestamp as tsStart, e.timestamp as tsStop, s.sid as playerId, " +
                    "'stop' as intensity, t[0].velocity/1000000 as instantSpeed, " +
                    "debs:getTwoDimDistance(s.pos_x, s.pos_y, e.pos_x, e.pos_y)/1000 as Distance, " +
                    "(e.timestamp - s.timestamp)/1000000000 as unitPeriod " +
                    "insert into RunningStats partition by Player;");

            m_SiddhiManager.addQuery("from s = GameStream [velocity <= 1000000 or velocity > 11000000] , " +
                    "     t = GameStream [velocity > 1000000 and velocity <= 11000000]+ , " +
                    "     e = GameStream [(velocity <= 1000000 or velocity > 11000000) and ((timestamp - s.timestamp) > 1000000000000L)] " +
                    "select s.timestamp as tsStart, e.timestamp as tsStop, s.sid as playerId, " +
                    "'trot' as intensity, t[0].velocity/1000000 as instantSpeed, " +
                    "debs:getTwoDimDistance(s.pos_x, s.pos_y, e.pos_x, e.pos_y)/1000 as Distance, " +
                    "(e.timestamp - s.timestamp)/1000000000 as unitPeriod " +
                    "insert into RunningStats partition by Player;");

            m_SiddhiManager.addQuery("from s = GameStream [velocity <= 11000000 or velocity > 14000000] , " +
                    "     t = GameStream [velocity > 11000000 and velocity <= 14000000]+ , " +
                    "     e = GameStream [(velocity <= 11000000 or velocity > 14000000) and ((timestamp - s.timestamp) > 1000000000000L)] " +
                    "select s.timestamp as tsStart, e.timestamp as tsStop, s.sid as playerId, " +
                    "'low' as intensity, t[0].velocity/1000000 as instantSpeed, " +
                    "debs:getTwoDimDistance(s.pos_x, s.pos_y, e.pos_x, e.pos_y)/1000 as Distance, " +
                    "(e.timestamp - s.timestamp)/1000000000 as unitPeriod " +
                    "insert into RunningStats partition by Player;");

            m_SiddhiManager.addQuery("from s = GameStream [velocity <= 14000000 or velocity > 17000000] , " +
                    "     t = GameStream [velocity > 14000000 and velocity <= 17000000]+ , " +
                    "     e = GameStream [(velocity <= 14000000 or velocity > 17000000) and ((timestamp - s.timestamp) > 1000000000000L)] " +
                    "select s.timestamp as tsStart, e.timestamp as tsStop, s.sid as playerId, " +
                    "'medium' as intensity, t[0].velocity/1000000 as instantSpeed, " +
                    "debs:getTwoDimDistance(s.pos_x, s.pos_y, e.pos_x, e.pos_y)/1000 as Distance, " +
                    "(e.timestamp - s.timestamp)/1000000000 as unitPeriod " +
                    "insert into RunningStats partition by Player;");

            m_SiddhiManager.addQuery("from s = GameStream [velocity <= 17000000 or velocity > 24000000] , " +
                    "     t = GameStream [velocity > 17000000 and velocity <= 24000000]+ , " +
                    "     e = GameStream [(velocity <= 17000000 or velocity > 24000000) and ((timestamp - s.timestamp) > 1000000000000L)] " +
                    "select s.timestamp as tsStart, e.timestamp as tsStop, s.sid as playerId, " +
                    "'high' as intensity, t[0].velocity/1000000 as instantSpeed, " +
                    "debs:getTwoDimDistance(s.pos_x, s.pos_y, e.pos_x, e.pos_y)/1000 as Distance, " +
                    "(e.timestamp - s.timestamp)/1000000000 as unitPeriod " +
                    "insert into RunningStats partition by Player;");

            m_SiddhiManager.addQuery("from s = GameStream [velocity <= 24000000] , " +
                    "     t = GameStream [velocity > 24000000]+ , " +
                    "     e = GameStream [(velocity <= 24000000) and ((timestamp - s.timestamp) > 1000000000000L)] " +
                    "select s.timestamp as tsStart, e.timestamp as tsStop, s.sid as playerId, " +
                    "'sprint' as intensity, t[0].velocity/1000000 as instantSpeed, " +
                    "debs:getTwoDimDistance(s.pos_x, s.pos_y, e.pos_x, e.pos_y)/1000 as Distance, " +
                    "(e.timestamp - s.timestamp)/1000000000 as unitPeriod " +
                    "insert into RunningStats partition by Player;");


            m_SiddhiManager.definePartition("define partition RunningPlayer by RunningStats.playerId");

            m_SiddhiManager.addQuery("from RunningStats[intensity == 'stop']#window.time(1 min) " +
                            "select max(tsStop) as ts, playerId, sum(convert(unitPeriod,double)) as totalTime, sum(convert(Distance,double)) as totalDistance " +
                            "insert into StopAggregateStats partition by RunningPlayer;"
            );

            m_SiddhiManager.addQuery("from RunningStats[intensity == 'trot']#window.time(1 min) " +
                            "select max(tsStop) as ts, playerId, sum(convert(unitPeriod,double)) as totalTime, sum(convert(Distance,double)) as totalDistance " +
                            "insert into TrotAggregateStats partition by RunningPlayer;"
            );

            m_SiddhiManager.addQuery("from RunningStats[intensity == 'low']#window.time(1 min) " +
                            "select max(tsStop) as ts, playerId, sum(convert(unitPeriod,double)) as totalTime, sum(convert(Distance,double)) as totalDistance " +
                            "insert into LowAggregateStats partition by RunningPlayer;"
            );

            m_SiddhiManager.addQuery("from RunningStats[intensity == 'medium']#window.time(1 min) " +
                            "select max(tsStop) as ts, playerId, sum(convert(unitPeriod,double)) as totalTime, sum(convert(Distance,double)) as totalDistance " +
                            "insert into MediumAggregateStats partition by RunningPlayer;"
            );

            m_SiddhiManager.addQuery("from RunningStats[intensity == 'high']#window.time(1 min) " +
                            "select max(tsStop) as ts, playerId, sum(convert(unitPeriod,double)) as totalTime, sum(convert(Distance,double)) as totalDistance " +
                            "insert into HighAggregateStats partition by RunningPlayer;"
            );

            m_SiddhiManager.addQuery("from RunningStats[intensity == 'sprint']#window.time(1 min) " +
                            "select max(tsStop) as ts, playerId, sum(convert(unitPeriod,double)) as totalTime, sum(convert(Distance,double)) as totalDistance " +
                            "insert into SprintAggregateStats partition by RunningPlayer;"
            );

//        	List<Attribute> lst = m_SiddhiManager.getStreamDefinition("RunningStats").getAttributeList();
//        	for (Attribute attribute : lst) {
//				System.out.println(attribute.getName() + " " + attribute.getType().toString());
//			}
        } catch (SiddhiParserException ex) {
            ex.printStackTrace();
        }

        StreamCallback mCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                m_Log.info("Result Event : " + Arrays.deepToString(events));

                l_ResultCounter += events.length;
                if (b_FirstOutput) {
                    b_FirstOutput = false;
                    m_StartTime = System.currentTimeMillis();
                } else {
                    m_EndTime = System.currentTimeMillis();
                }
            }
        };

        m_SiddhiManager.addCallback("RunningStats", mCallback);
        m_SiddhiManager.addCallback("StopAggregateStats", mCallback);
        m_SiddhiManager.addCallback("TrotAggregateStats", mCallback);
        m_SiddhiManager.addCallback("LowAggregateStats", mCallback);
        m_SiddhiManager.addCallback("MediumAggregateStats", mCallback);
        m_SiddhiManager.addCallback("HighAggregateStats", mCallback);
        m_SiddhiManager.addCallback("SprintAggregateStats", mCallback);
    }

    private void SetupFilterQuery() {
        try {
            m_SiddhiManager.addQuery("from  GameStream [sid == 4 or sid == 8] " +
                    "select sid, timestamp, pos_x, pos_y, pos_z " +
                    "insert into PlayerStream;");
        } catch (SiddhiParserException ex) {
            ex.printStackTrace();
        }

        StreamCallback mCallback = new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                m_Log.info("Result Event : " + Arrays.deepToString(events));

                l_ResultCounter += events.length;
                if (b_FirstOutput) {
                    b_FirstOutput = false;
                    m_StartTime = System.currentTimeMillis();
                } else {
                    m_EndTime = System.currentTimeMillis();
                }
            }
        };

        m_SiddhiManager.addCallback("PlayerStream", mCallback);
    }

    public void Start() {
        m_DataFeeder.run();

		try {
			Thread.sleep(1000);
		} catch(InterruptedException ex) {
			ex.printStackTrace();
		}
        m_SiddhiManager.shutdown();

        System.out.println("Input event count  : " + m_DataFeeder.GetEventCounter());
        System.out.println("Result event count : " + l_ResultCounter);
        long lInputSpan = m_DataFeeder.l_EndTime - m_DataFeeder.l_StartTime;
        float fThroughput = (m_DataFeeder.GetEventCounter() / lInputSpan * 1000);
        System.out.println("Input  : Start=" + m_DataFeeder.l_StartTime + " End=" + m_DataFeeder.l_EndTime +
                " Span=" + lInputSpan + " Throughput=" + fThroughput);
        long lOutputSpan = m_EndTime - m_StartTime;
        fThroughput = (m_DataFeeder.GetEventCounter() / lOutputSpan * 1000);
        System.out.println("Output : Start=" + m_StartTime + " End=" + m_EndTime + " Span=" + lOutputSpan +
                " Throughput=" + fThroughput);

        System.out.println("Event monitor : StatStartTime=" + m_EventMonitor.getStatStartTime());
        System.out.println("Event monitor : LastUpdateTime=" + m_EventMonitor.getLastUpdateTime());
        long lEventMonitorSpan = m_EventMonitor.getLastUpdateTime() - m_EventMonitor.getStatStartTime();
        if (lEventMonitorSpan == 0) {
            lEventMonitorSpan = 1;
        }
        fThroughput = (m_DataFeeder.GetEventCounter() / lEventMonitorSpan * 1000);
        System.out.println("Event monitor : Span=" + lEventMonitorSpan + " Throughput=" + fThroughput);
        for (Map.Entry<String, AtomicLong> o : m_EventMonitor.getStreamStats().entrySet()) {
            System.out.println("Event monitor : Key=" + o.getKey() + " Value=" + o.getValue());
        }
    }


    public static void main(String[] args) {
        //DataFeeder mFeeder = new SocketReader("127.0.0.1", 9501);
        DataFeeder mFeeder = new FileFeeder(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        App mApp = new App(mFeeder);
        mApp.Start();
    }
}