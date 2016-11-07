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

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhengqh on 16/8/19.
 */
public class HashApp
{

    public static void main(String[] args)
    {
        Set<String> nodes = new HashSet<String>();

        //服务器列表，3个服务器，放入一致性hash环
        nodes.add("10.10.80");
        nodes.add("10.10.70");
        nodes.add("10.10.60");
        //初始化一致性hash
        ConsistentHash<String> consistentHash = new ConsistentHash<String>(new HashFunction(), 160, nodes);

        consistentHash.add("D");
        System.out.println(consistentHash.getSize());  //640=4*160

        System.out.println(consistentHash.get("1"));
        System.out.println(consistentHash.get("2"));
        System.out.println(consistentHash.get("3"));
        System.out.println(consistentHash.get("4"));
    }
}
