/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

/**
 * @author zxb 2014年12月29日 下午3:46:23

CREATE KEYSPACE forseti WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} ;

CREATE TABLE forseti.mytab (
id text PRIMARY KEY,
age int,
names list<text>
) WITH bloom_filter_fp_chance = 0.01
AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
AND comment = ''
AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
AND default_time_to_live = 0
AND gc_grace_seconds = 864000
AND max_index_interval = 2048
AND memtable_flush_period_in_ms = 0
AND min_index_interval = 128
AND read_repair_chance = 0.0
AND speculative_retry = '99.0PERCENTILE';


insert into forseti.mytab(id,age,names)values('a1',10,['01']);
insert into forseti.mytab(id,age,names)values('a2',20,['01','02']);
insert into forseti.mytab(id,age,names)values('a3',30,['01','02','03']);

cqlsh:forseti> select * from mytab;

id | age | names
----+-----+--------------------
a3 |  30 | ['01', '02', '03']
a2 |  20 |       ['01', '02']
a1 |  10 |             ['01']

(3 rows)

 */
public class CassandraTest {

    public static void main(String[] args) {
        Builder builder = Cluster.builder();
        builder.addContactPoint("127.0.0.1");

        // socket 链接配置
        // 为了调度时不至于很快中断，把超时时间设的长一点
        SocketOptions socketOptions = new SocketOptions().setKeepAlive(true).setConnectTimeoutMillis(5 * 10000).setReadTimeoutMillis(100000);
        builder.withSocketOptions(socketOptions);
        Cluster cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }

        Session session = cluster.connect();

        ResultSet results = session.execute("SELECT * FROM forseti.mytab where id='a1'");
        for (Row row : results) {
            System.out.println(String.format("%-10s\t%-10s\t%-20s", row.getString("id"), row.getInt("age"),
                                             row.getList("names", String.class)));
        }
        cluster.close();
    }
}