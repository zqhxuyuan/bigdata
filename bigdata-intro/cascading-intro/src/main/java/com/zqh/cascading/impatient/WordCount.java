package com.zqh.cascading.impatient;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * http://docs.cascading.org/impatient/impatient2.html
 *
 $ hadoop fs -cat /output/cascading/wordcount/part-00000
 token	count
 9
 A	3
 Australia	1
 Broken	1
 California's	1
 DVD	1
 Death	1
 Land	1
 Secrets	1
 This	2
 Two	1
 Valley	1
 Women	1
 a	5
 air	1
 an	1
 and	2
 area	4
 as	2
 back	1
 cause	1
 cloudcover	1
 deserts	1
 downwind	1
 dry	3
 effect	1
 in	1
 is	4
 known	1
 land	1
 lee	2
 leeward	2
 less	1
 lies	1
 mountain	3
 mountainous	1
 of	6
 on	2
 or	2
 primary	1
 produces	1
 rain	5
 ranges	1
 shadow	4
 side	2
 sinking	1
 such	1
 that	1
 the	5
 with	1

 */
public class WordCount {

    public static void  main( String[] args ) {
        args = new String[]{"/input/cascading/impatient/rain.txt","/output/cascading/wordcount",};

        String docPath = args[ 0 ];
        String wcPath = args[ 1 ];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass( properties, WordCount.class );
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

        // create source and sink taps
        Tap docTap = new Hfs( new TextDelimited( true, "\t" ), docPath );
        Tap wcTap = new Hfs( new TextDelimited( true, "\t" ), wcPath );

        // specify a regex operation to split the "document" text lines into a token stream
        Fields token = new Fields( "token" );
        Fields text = new Fields( "text" );
        RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
        // only returns "token"
        Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );

        // determine the word counts
        Pipe wcPipe = new Pipe( "wc", docPipe );
        wcPipe = new GroupBy( wcPipe, token );
        wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .setName( "wc" )
                .addSource( docPipe, docTap )
                .addTailSink( wcPipe, wcTap );

        // write a DOT file and run the flow
        Flow wcFlow = flowConnector.connect( flowDef );
        wcFlow.writeDOT( "dot/wc.dot" );
        wcFlow.complete();
    }
}
