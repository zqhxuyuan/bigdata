package com.github.miguelantonio;

/*
 * Copyright 2015 Variacode
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.junit.Test;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.Token;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class EsperBoltStressTest {

    private static long startMilis;
    private static long endMilis;
    private static long tuplesCount;
    private static final String[] header = {"id", "symbol", "datetime", "buyPrice", "sellPrice", "type"};
    private static final String LITERAL_RETURN_OBJ = "Result";
    private static final String LITERAL_ESPER = "esper";
    private static final String LITERAL_QUOTES = "quotes";
    private static final boolean DEBUG = false;
    private static final int MAX_INPUT_FILES = 1;

    private static void write(String msg) {
        Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.INFO, msg);
    }

    private static void log(String msg) {
        if (DEBUG) {
            write(msg);
        }
    }

    @Test
    public void testEPL() {

        Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.INFO, "EngineTest-EPL");
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(LITERAL_QUOTES, new SpreadSpout());
        builder.setBolt(LITERAL_ESPER, (new EsperBolt())
                .addEventTypes(ForexSpreadTestBean.class)
                .addOutputTypes(Collections.singletonMap(LITERAL_RETURN_OBJ,
                        Arrays.asList("avg", "buyPrice")))
                .addStatements(Collections.singleton("insert into Result "
                        + "select avg(buyPrice) as avg, buyPrice from "
                        + "quotes_default(symbol='AUD/USD').win:length(2) "
                        + "having avg(buyPrice) > 1.0")))
                .shuffleGrouping(LITERAL_QUOTES);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping(LITERAL_ESPER, LITERAL_RETURN_OBJ);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(10000);
        cluster.shutdown();
        write("TUPLAS PROCESADAS: " + tuplesCount);
        write("MILISEGUNDOS: " + (endMilis - startMilis));
        write("VELOCIDAD: " + (tuplesCount / (endMilis - startMilis)) + " TUPLAS/MILISEGUNDO");
        assertEquals(true, true);
    }

    public static class ComparableFileByModified implements Comparable {
        public long t;
        public File f;

        public ComparableFileByModified(File file) {
            f = file;
            t = file.lastModified();
        }

        @Override
        public int compareTo(Object o) {
            long u = ((ComparableFileByModified) o).t;
            return t < u ? -1 : t == u ? 0 : 1;
        }
    };

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            log("PRICE: " + tuple.getDoubleByField("buyPrice") + " - AVG: " + tuple.getDoubleByField("avg"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {
            //Not implemented
        }
    }

    public static class SpreadSpout extends BaseRichSpout {

        transient SpoutOutputCollector collector;
        transient CellProcessor[] processors;
        transient ICsvBeanReader[] beanReader;
        transient int beanReaderCount;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

            this.collector = collector;
            CellProcessor[] p = {new Optional(), new Optional(),
                    new Optional(new Token(" ", null, new ParseDate("yyyy-MM-dd HH:mm:ss.SSS", true))),
                    new Optional(new ParseDouble()), new Optional(new ParseDouble()), new Optional()};
            this.processors = p;
            File[] files = (new File("src/test/resources")).listFiles();
            ComparableFileByModified[] pairs = new ComparableFileByModified[files.length];
            for (int i = 0; i < files.length; i++) {
                pairs[i] = new ComparableFileByModified(files[i]);
            }
            Arrays.sort(pairs);
            this.beanReader = new CsvBeanReader[files.length];
            for (int i = 0; i < pairs.length; i++) {
                try {
                    this.beanReader[i] = new CsvBeanReader(new FileReader(pairs[i].f), CsvPreference.STANDARD_PREFERENCE);
                } catch (FileNotFoundException ex) {
                    Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.SEVERE, null, ex);
                    this.beanReader[i] = null;
                }
            }
            this.beanReaderCount = 0;
            startMilis = System.currentTimeMillis();
        }

        @Override
        public void nextTuple() {
            if (beanReaderCount < this.beanReader.length && beanReaderCount < MAX_INPUT_FILES) {
                if (this.beanReader[beanReaderCount] != null) {
                    tuplesCount++;
                    ForexSpreadTestBean spread;
                    try {
                        if ((spread = beanReader[beanReaderCount].read(ForexSpreadTestBean.class, EsperBoltStressTest.header, processors)) != null) {
                            log(String.format("lineNo=%s, rowNo=%s, spread=%s", beanReader[beanReaderCount].getLineNumber(),
                                    beanReader[beanReaderCount].getRowNumber(), spread));
                            this.collector.emit(new Values(spread.getId(), spread.getSymbol(), spread.getDatetime(), spread.getBuyPrice(), spread.getSellPrice(), spread.getType()));
                            endMilis = System.currentTimeMillis();
                        }
                    } catch (IOException ex) {
                        Logger.getLogger(EsperBoltStressTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else {
                    beanReaderCount++;
                }
            }
        }

        @Override
        public void ack(Object id) {
            //Not implemented
        }

        @Override
        public void fail(Object id) {
            //Not implemented
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(EsperBoltStressTest.header));
        }

    }

}
