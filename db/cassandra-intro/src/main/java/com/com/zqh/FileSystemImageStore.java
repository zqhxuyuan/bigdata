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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zhengqh on 16/8/25.
 */
public class FileSystemImageStore {

    public void write(String location, ByteBuffer blob) throws IOException
    {

        ByteBuffer buf = ByteBuffer.allocate(blob.limit());
        buf.clear();
        buf.put(blob);
        buf.flip();

        RandomAccessFile file = new RandomAccessFile(location, "rw");
        FileChannel channel = file.getChannel();
        try {
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        }finally {
            channel.force(true);
            channel.close();
        }
    }

    public ByteBuffer read(String location) throws IOException {
        RandomAccessFile file = new RandomAccessFile(location, "r");
        FileChannel channel = file.getChannel();
        ByteBuffer buf = ByteBuffer.allocate((int)channel.size());

        try {
            while(channel.read(buf) > 0  ) {
                buf.flip();
                buf.clear();
            }
        }finally{
            channel.force(true);
            channel.close();
        }
        return buf;
    }
}