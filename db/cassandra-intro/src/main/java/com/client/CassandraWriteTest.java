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

package com.client;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;

public class CassandraWriteTest {

    public static void main(String[] args) {
        Builder builder = Cluster.builder();
        builder.addContactPoint("127.0.0.1");

        // socket 链接配置
        SocketOptions socketOptions = new SocketOptions().setKeepAlive(true).setConnectTimeoutMillis(5 * 10000).setReadTimeoutMillis(100000);
        builder.withSocketOptions(socketOptions);
        Cluster cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        Session session = cluster.connect();

        for (int i = 0; i < 1; i++) {
            String id = "c" + i;
            String names = "name" + i;
            String sql = "insert into forseti.mytab (id,age,names) values('" + id + "'," + i + ",['" + names + "'])";
            session.execute(sql);
        }
        cluster.close();
    }
}