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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zhengqh on 16/8/25.
 * https://github.com/datastax/code-samples/tree/master/blobs_java_driver/src/main/java
 *
 */
public class LoadImageStress
{

    public static void main(String[] args) throws ClassNotFoundException, IOException
    {

        CassandraImageStore cassandraImageStore = new CassandraImageStore();
        FileSystemImageStore fileSystemImageStore = new FileSystemImageStore();
        try {
            String userHome = System.getProperty("user.home");
            //读取本地文件
            String file2 = userHome + "/data/test.dat";
            ByteBuffer imageBytes = fileSystemImageStore.read(file2);
            //写入到Cassandra
            long start = System.currentTimeMillis();
            for(int i=0;i<1000;i++){
                cassandraImageStore.storeImage(imageBytes, i + "");
            }
            long end = System.currentTimeMillis();

            for(int i=0;i<1000;i++){
                ByteBuffer byteBuffer = cassandraImageStore.getImage( i + "");
            }
            long end2 = System.currentTimeMillis();

            System.out.println("Write:" + (end -start)/1000);
            System.out.println("Read:" + (end2 -end)/1000);

            System.exit(0);
        }finally{
            cassandraImageStore.shutDown();
        }
    }

}