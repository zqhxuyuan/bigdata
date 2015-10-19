/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.api.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.calrissian.flowmix.core.model.StreamDef;
import org.calrissian.flowmix.core.model.op.FlowOp;
import org.calrissian.flowmix.core.model.op.JoinOp;

public class FlowDefsBuilder {

    private List<StreamDef> streamList = new ArrayList<StreamDef>();
    private FlowBuilder flowBuilder;

    public FlowDefsBuilder(FlowBuilder flowBuilder) {
        this.flowBuilder = flowBuilder;
    }

    public List<StreamDef> getStreamList() {
        return streamList;
    }

    //一个Flow可以配置多个Stream流
    protected void addStream(StreamDef stream) {
        streamList.add(stream);
    }

    //每个流定义StreamDef用StreamBuilder构建模式创建. 正如Flow由FlowBuilder创建
    public StreamBuilder stream(String name) {
        return new StreamBuilder(this, name, true);
    }

    public StreamBuilder stream(String name, boolean stdInput) {
        return new StreamBuilder(this, name, stdInput);
    }

    public FlowBuilder endDefs() {

      boolean oneStdOut = false;
      boolean oneStdIn = false;

      //StreamDef的output -> Set(StreamDef.name)
      Map<String, Set<String>> inputs = new HashMap<String, Set<String>>();
      for(StreamDef def : getStreamList()) {
        if(def.isStdInput())
          oneStdIn = true;

        if(def.isStdOutput())
          oneStdOut = true;

        if(def.getOutputs() != null) {
          for(String output : def.getOutputs()) {
            Set<String> entry = inputs.get(output);
            if(entry == null) {
              entry = new HashSet<String>();
              inputs.put(output, entry);
            }
            entry.add(def.getName());
          }
        }
      }

      if(!oneStdIn)
        throw new RuntimeException("At least one stream needs to read from std input");

      if(!oneStdOut)
        throw new RuntimeException("At least one stream needs to write to std output");

      for(StreamDef def : getStreamList()) {
        //每个StreamDef由多个FlowOp操作符组成
        for(FlowOp op : def.getFlowOps()) {
          if(op instanceof JoinOp) {
            String lhs = ((JoinOp) op).getLeftStream();
            String rhs = ((JoinOp) op).getRightStream();

            Set<String> outputs = inputs.get(def.getName());
            if(!outputs.contains(lhs) || !outputs.contains(rhs))
              throw new RuntimeException("A join operator was found but the necessary streams are not being routed to it. Offending stream: " + def.getName());
          }
        }
      }

      return flowBuilder;
    }
}
