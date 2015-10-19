/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zqh.cascading.impatient;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import java.util.Properties;

/**
 * http://docs.cascading.org/impatient/impatient1.html
 *
 * Notice:
 * 由于在本项目中有多个hadoop相关的生态系统项目,各个系统之中使用的hadoop版本不相同.
 * 可以使用exclusions来排除某个生态系统依赖的hadoop版本,然后手动声明自己目前运行的hadoop版本
 * 比如这里我们使用了
 <dependency>
     <groupId>cascading</groupId>
     <artifactId>cascading-hadoop2-mr1</artifactId>
     <version>${cascading.version}</version>
     <exclusions>
         <exclusion>
             <artifactId>hadoop-mapreduce-client-common</artifactId>
             <groupId>org.apache.hadoop</groupId>
         </exclusion>
     </exclusions>
 </dependency>

 $ hadoop fs -cat /output/cascading/rain1/part-00000
 doc_id	text
 doc01	A rain shadow is a dry area on the lee back side of a mountainous area.
 doc02	This sinking, dry air produces a rain shadow, or area in the lee of a mountain with less rain and cloudcover.
 doc03	A rain shadow is an area of dry land that lies on the leeward (or downwind) side of a mountain.
 doc04	This is known as the rain shadow effect and is the primary cause of leeward deserts of mountain ranges, such as California's Death Valley.
 doc05	Two Women. Secrets. A Broken Land. [DVD Australia]

 */
public class Copy {

    public static void main(String[] args) {
        // local
        args = new String[]{"data/cascading/rain.txt", "output/rain"};
        // hdfs
        args = new String[]{"/input/cascading/impatient/rain.txt", "/output/cascading/copy",};

        String inPath = args[0];
        String outPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Copy.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        // create the source tap
        Tap inTap = new Hfs(new TextDelimited(true, "\t"), inPath);

        // create the sink tap
        Tap outTap = new Hfs(new TextDelimited(true, "\t"), outPath);

        // specify a pipe to connect the taps
        Pipe copyPipe = new Pipe("copy");

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .addSource(copyPipe, inTap)
                .addTailSink(copyPipe, outTap);

        // run the flow
        flowConnector.connect(flowDef).complete();
    }
}
