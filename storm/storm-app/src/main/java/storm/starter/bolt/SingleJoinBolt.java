/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TimeCacheMap;

import java.util.*;

public class SingleJoinBolt extends BaseRichBolt {
    OutputCollector _collector;
    Fields _idFields;   //参与join的字段,可以有多个,比如这里只有一个:[id]
    Fields _outFields;  //输出字段列表,比如这里的[gender,age]
    int _numSources;    //参与join的数据源
    //将接收到的tuple暂时保存在内存中.只有收到相同id的map元素个数和source个数一样时,才从中删除
    TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>> _pending;
    Map<String, GlobalStreamId> _fieldLocations;  //输出字段对应的source

    public SingleJoinBolt(Fields outFields) {
        _outFields = outFields;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _fieldLocations = new HashMap<String, GlobalStreamId>();
        _collector = collector;
        int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
        _pending = new TimeCacheMap<List<Object>, Map<GlobalStreamId, Tuple>>(timeout, new ExpireCallback());
        //参与join的数据源. 这里因为是[id,gender]和[id,age],所以值为2
        _numSources = context.getThisSources().size();

        //参与join的字段. 在实际中可以有多个,这里只有一个id
        Set<String> idFields = null;
        //对于不同的源
        for (GlobalStreamId source : context.getThisSources().keySet()) {
            //每个源的输出字段. 如果是gender这个source,则输出字段是[id,gender]. 同样如果是age source,输出字段为[id,age]
            //在之前的Bolt中声明输出字段是这样的:new Fields("id","gender").所以这里也是一样的.获取到的就是这样的Fields对象
            Fields fields = context.getComponentOutputFields(source.get_componentId(), source.get_streamId());
            //如果字段Fields有多个参数,比如("id","gender"),则通过调用toList转成Set集合形式
            Set<String> setFields = new HashSet<String>(fields.toList());
            if (idFields == null)
                idFields = setFields; //处理第一个source时,idFields=null,另idFields=第一个Fields
            else
                //[id,gender] retainAll [id,age] == id
                idFields.retainAll(setFields);

            //_outFields是构造函数的[gender,age]
            for (String outfield : _outFields) {
                //fields是当前source的输入字段,比如[id,gender]
                for (String sourcefield : fields) {
                    if (outfield.equals(sourcefield)) {
                        //gender,gender_source_stream-id
                        //age,age_source_stream-id
                        //即输出字段属于哪个source
                        _fieldLocations.put(outfield, source);
                    }
                }
            }
        }
        _idFields = new Fields(new ArrayList<String>(idFields));

        if (_fieldLocations.size() != _outFields.size()) {
            throw new RuntimeException("Cannot find all outfields among sources");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        //每一个接收到的tuple,在上一个Spout或者Bolt中都定义了输出字段. 因为这里Bolt的上一个是Spout,分别定义了[id,gender]和[id,age]输出字段
        //所以接收tuple可以通过tuple.getInteger("id")或者tuple.getString("gender")获得对应的值. 也可以采用下面的select方式,传递的是Fields对象

        //通过tuple.getInteger("id")获取的是单个tuple在id字段的值.
        //为什么返回的是List<Object>? 下面的解释对不对? --- X
        //这里采用join,因为有两个source,所以在一个execute(tuple)方法调用中会接收到两个source发送过来的tuple.
        //其中id相等的tuple会在这里被一起处理. 所以返回的List对象表示了多个source在id这个字段的值
        //List<Object>只是用来表示参与join的字段.如果有多个字段要进行join,则有多个值,用List来保存.为什么是Object,因为join的类型可能是int,或者string
        //注意execute(tuple)每次只会被一个source的一个tuple调用.不会说相同id的多个tuple在本次execute(tuple)中被一起处理! 因为tuple就是单个的意思!
        List<Object> id = tuple.select(_idFields);
        //每个Tuple都属于对应的source. 注意属于同一个Component的所有tuple,它的GlobalStreamId是一样的
        GlobalStreamId streamId = new GlobalStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId());
        if (!_pending.containsKey(id)) {
            //在构造函数中的初始化只是初始化了外层的Map, 这里要往id对应的Map中添加记录,要对里层的Map也要初始化
            //pending这个map用来保存相同id的多个数据源. 只有当map中id对应的值:内层map的个数等于source个数,才认为接收到了这个id的所有source的数据,才可以开始join!
            _pending.put(id, new HashMap<GlobalStreamId, Tuple>());
        }
        Map<GlobalStreamId, Tuple> parts = _pending.get(id);
        if (parts.containsKey(streamId))
            throw new RuntimeException("Received same side of single join twice");
        parts.put(streamId, tuple);

        //当保存在_pending对应id的Map中的记录数=source的数量时, 说明已经接收到了相同id的多个不同source
        //就像word-count中用counts来保存word->counts的映射关系:每次处理一个tuple,并把当前tuple对应的word在counts中的计数+1
        //这里用_pending来记录状态. 在还没有接收到相同id对应的所有source之前,不能开始join.
        //比如只接收到了[id,gender]=[1,M],还没收到[id,age]=[1,20]之前,显然没办法进行id=1的join操作.
        if (parts.size() == _numSources) {
            //已经接收到了参与当前id进行join的所有source了.可以从pending中移除了.然后开始join!
            _pending.remove(id);
            List<Object> joinResult = new ArrayList<Object>();
            for (String outField : _outFields) { //gender,age
                //输出字段来源于哪个source
                GlobalStreamId loc = _fieldLocations.get(outField);
                //parts.get(loc)得到的是内存map中的tuple. 即输出字段-->source-->tuple(intput,receive)-->outField-->intput data for this field
                //这里实际上才是真正的获取tuple里的内容: tuple.getString("gender")
                joinResult.add(parts.get(loc).getValueByField(outField));
            }
            _collector.emit(new ArrayList<Tuple>(parts.values()), joinResult);

            for (Tuple part : parts.values()) {
                _collector.ack(part);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_outFields);
    }

    //Storm的TimeCacheMap,定义了一个失效策略的回调函数. 当map里的元素失效时,怎么处理这些元素. 这里是直接fail掉tuple
    private class ExpireCallback implements TimeCacheMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> {
        @Override
        public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) {
            for (Tuple tuple : tuples.values()) {
                _collector.fail(tuple);
            }
        }
    }
}
