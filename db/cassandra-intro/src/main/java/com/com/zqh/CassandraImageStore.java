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

package com.com.zqh;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Created by zhengqh on 16/8/25.
 */
public class CassandraImageStore {

    private final Session session;
    private final Cluster cluster;

    public CassandraImageStore(){
        cluster = Cluster.builder()
                         .addContactPoint("192.168.6.53")
                         .build();
        session = cluster.connect("keyspace1");
    }
    public void storeImage(ByteBuffer fileBlob, String imageId){
        session.execute("INSERT INTO images ( image_id, image) values ( ?, ? )", imageId, fileBlob);
    }

    public ByteBuffer getImage(String imageId){
        ResultSet rows = session.execute("SELECT image FROM images WHERE image_id = ?", imageId);
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        for(Row row: rows){
            buffers.add(row.getBytes("image"));
        }
        if(buffers.size() == 1){
            return buffers.get(0);
        }else if(buffers.size()>1){
            throw new RuntimeException("More than one matching image for id '" + imageId + "' found");
        }
        throw new RuntimeException("None matching images for id '" + imageId + "' found");
    }

    public void shutDown(){
        session.close();
        cluster.close();
    }
}