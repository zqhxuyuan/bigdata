package org.wso2.siddhi.storm.components;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class StockDataSpout extends BaseRichSpout {
    private static final int EVENT_SIZE = 1000000;
    private transient Log log = LogFactory.getLog(StockDataSpout.class);
    private SpoutOutputCollector _collector;
    private boolean useDefaultAsStreamName = true;
    private String[] stockSymbols = new String[]{
            "WSO2", "FB", "GOOG", "LNKD", "AMD", "NVIDIA", "APPL", "YAHOO", "INTEL", "SAMSUNG"
    };
    private Map<String, Double> stockPrices = new HashMap<String, Double>();
    private Map<String, Integer> stockVolumes = new HashMap<String, Integer>();
    private List<Object[]> eventQueue = new ArrayList<Object[]>(EVENT_SIZE);

    int nextIndex = 0;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (useDefaultAsStreamName) {
            declarer.declare(new Fields("symbol", "price", "volume"));
        } else {
            declarer.declareStream("StockStream", new Fields("symbol", "price", "volume"));
        }
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this._collector = collector;
        Random rand = new Random();
        for (String stockSymbol : stockSymbols) {
            double price = Math.round(rand.nextDouble() * 100000) / 100D;
            int volume = rand.nextInt(10000);
            stockPrices.put(stockSymbol, price);
            stockVolumes.put(stockSymbol, volume);
        }
        try {
            generateNextEvents(EVENT_SIZE);
        } catch (InterruptedException e) {
            log.error("Thread interrupted : " + e.getMessage());
        }
    }


    @Override
    public void nextTuple() {
        Object[] data = eventQueue.get(nextIndex);
        if (useDefaultAsStreamName) {
            _collector.emit(new Values(data));
        } else {
            _collector.emit("StockData", new Values(data));
        }
        nextIndex++;
        if (nextIndex == EVENT_SIZE){
            nextIndex = 0;
        }
    }


    private void generateNextEvents(int size) throws InterruptedException {
        for (int i = 0; i < size; i++) {
            Random rand = new Random();
            int symbolIndex = rand.nextInt(stockSymbols.length);
            String stockSymbol = stockSymbols[symbolIndex];
            Double price = stockPrices.get(stockSymbol);
            Integer volume = stockVolumes.get(stockSymbol);

            eventQueue.add(new Object[]{stockSymbol, price, volume});

            if (rand.nextInt() % 2 == 0) {
                stockPrices.put(stockSymbol, Math.round(price * (1 + rand.nextDouble() * 0.3) * 100) / 100D);
                stockVolumes.put(stockSymbol, Math.round(volume * (1 + rand.nextFloat())));
            } else {
                stockPrices.put(stockSymbol, Math.round(price * (1 - rand.nextDouble() * 0.3) * 100) / 100D);
                stockVolumes.put(stockSymbol, Math.round(volume * (1 - rand.nextFloat())));
            }
        }
    }
}