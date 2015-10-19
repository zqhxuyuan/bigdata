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
import java.util.List;

import org.calrissian.flowmix.core.model.StreamDef;
import org.calrissian.flowmix.core.model.op.FlowOp;

public class StreamBuilder {

    private String name;
    private List<FlowOp> flowOpList = new ArrayList<FlowOp>();
    FlowDefsBuilder flowOpsBuilder;
    boolean stdInput;

    public StreamBuilder(FlowDefsBuilder flowOpsBuilder, String name, boolean stdInput) {
      this.flowOpsBuilder = flowOpsBuilder;
      this.name = name;
      this.stdInput = stdInput;
    }

    public boolean isStdInput() {
      return stdInput;
    }

    //通常会在op builder的end方法时,把当前自己的op加入
    protected void addFlowOp(FlowOp flowOp) {
        flowOpList.add(flowOp);
    }

    //支持的flow op操作符有: 过滤器, 选择, 聚合, 分区, 联合, 排序等
    public FilterBuilder filter() {
        //把StreamBuilder传入每个op的builder中,是为了在调用op builder的end时,返回StreamBuilder.
        //这样可以基于返回值继续调用其他op. 比如StreamBuilder.filter().end().select().end()....
        return new FilterBuilder(this);
    }

    public SelectBuilder select() {
        return new SelectBuilder(this);
    }

    public AggregateBuilder aggregate() {
        return new AggregateBuilder(this);
    }

    public PartitionBuilder partition() {
        return new PartitionBuilder(this);
    }

    public SwitchBuilder stopGate() {
        return new SwitchBuilder(this);
    }

    public JoinBuilder join(String stream1, String stream2) {
        return new JoinBuilder(this, stream1, stream2);
    }

    public SplitBuilder split() {
      return new SplitBuilder(this);
    }

    public SortBuilder sort() {
        return new SortBuilder(this);
    }

    public EachBuilder each() {
        return new EachBuilder(this);
    }

    public FlowDefsBuilder endStream() {
      return endStream(true, null);
    }

    /**
     * 结束一个Stream的定义.
     * 通过FlowDefsBuidler.stream创建StreamBuilder
     * 在完成一个流的定义后,还是在StreamBuilder上调用endStream
     * @param stdOutput 是不是标准的输出
     * @param outputs 如果不是标准输出,通常会输出到其他Stream中去
     * @return
     */
    public FlowDefsBuilder endStream(boolean stdOutput, String... outputs) {
      //流是以flow的操作符组成. 即多个op操作符组成一条流.
      StreamDef def = new StreamDef(name, flowOpList, stdInput, stdOutput, outputs);
      if(!def.isStdOutput() && def.getOutputs().length == 0)
        throw new RuntimeException("You must specify at least one output. Offending stream: " + name);

      flowOpsBuilder.addStream(def);
      return flowOpsBuilder;
    }

  protected List<FlowOp> getFlowOpList() {
    return flowOpList;
  }

  //标准输出流,也可能定义了输出目标
  public FlowDefsBuilder endStream(String... outputs) {
    return endStream(true, outputs);
  }

}
