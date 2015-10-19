package com.github.miguelantonio;

/*
 * Copyright 2015 Variacode
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.espertech.esper.client.*;
import com.espertech.esper.client.soda.EPStatementObjectModel;

import java.lang.reflect.Field;
import java.util.*;

/**
 * https://github.com/miguelantonio/storm-esper-bolt
 */
public class EsperBolt extends BaseRichBolt implements UpdateListener {
    private static final long serialVersionUID = 1L;

    private OutputCollector collector;
    private transient EPServiceProvider epService;
    private Map<String, List<String>> outputTypes;
    private Map<String, Object> eventTypes;
    private Set<String> statements;
    private Set<EPStatementObjectModel> objectStatements;

    public static void checkEPLSyntax(String statement) throws EsperBoltException {
        Configuration cepConfig = new Configuration();
        EPServiceProvider service = EPServiceProviderManager.getDefaultProvider(cepConfig);
        service.initialize();
        try {
            service.getEPAdministrator().prepareEPL(statement);
        } catch (EPStatementException e) {
            throw new EsperBoltException(e.getMessage(), e);
        }
        service.destroy();
    }

    public EsperBolt addOutputTypes(Map<String, List<String>> types) {
        String error = "Output Types cannot be null";
        exceptionIfAnyNull(error, types.values());
        for (List<String> s : types.values()) {
            exceptionIfAnyNull(error, s);
        }
        exceptionIfAnyNull(error, types.keySet());
        this.outputTypes = Collections.unmodifiableMap(exceptionIfNull(error, types));
        return this;
    }

    public EsperBolt addEventTypes(Class bean) {
        Map<String, Object> types = new HashMap<>();
        for (Field f : bean.getDeclaredFields()) {
            types.put(f.getName(), f.getType());
        }
        return addEventTypes(types);
    }

    public EsperBolt addEventTypes(Map<String, Object> types) {
        String error = "Event types cannot be null";
        this.eventTypes = Collections.unmodifiableMap(exceptionIfNull(error, types));
        exceptionIfAnyNull(error, types.values());
        exceptionIfAnyNull(error, types.keySet());
        return this;
    }

    public EsperBolt addStatements(Set<String> statements) {
        String error = "Statements cannot be null";
        this.statements = Collections.unmodifiableSet(exceptionIfNull(error, statements));
        exceptionIfAnyNull(error, statements);
        return this;
    }

    public EsperBolt addObjectStatemens(Set<EPStatementObjectModel> objectStatements) {
        final String error = "Object Statements cannot be null";
        this.objectStatements = Collections.unmodifiableSet(exceptionIfNull(error, objectStatements));
        exceptionIfAnyNull(error, objectStatements);
        return this;
    }

    private <O> O exceptionIfNull(String msg, O obj) {
        exceptionIfAnyNull(msg, obj);
        return obj;
    }

    private <O> void exceptionIfAnyNull(String msg, O... obj) {
        for (O o : obj) {
            if (o == null) {
                throw new FailedException(msg);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        if (this.outputTypes == null) {
            throw new FailedException("outputTypes cannot be null");
        }
        for (Map.Entry<String, List<String>> outputEventType : this.outputTypes.entrySet()) {
            List<String> fields = new ArrayList<>();
            if (outputEventType.getValue() != null) {
                for (String f : outputEventType.getValue()) {
                    fields.add(f);
                }
            } else {
                throw new FailedException();
            }
            ofd.declareStream(outputEventType.getKey(), new Fields(fields));
        }
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, OutputCollector oc) {
        this.collector = oc;
        Configuration cepConfig = new Configuration();
        if (this.eventTypes == null || (this.objectStatements == null && this.statements == null)) {
            throw new FailedException("Event types cannot be null and at least one type of statement has to be not null");
        }
        for (Map.Entry<GlobalStreamId, Grouping> a : tc.getThisSources().entrySet()) {
            Fields f = tc.getComponentOutputFields(a.getKey());
            if (!this.eventTypes.keySet().containsAll(f.toList())) {
                throw new FailedException("Event types and fields from source streams do not match: Event Types="
                        + Arrays.toString(this.eventTypes.keySet().toArray())
                        + " Stream Fields=" + Arrays.toString(f.toList().toArray()));
            }
            cepConfig.addEventType(a.getKey().get_componentId() + "_" + a.getKey().get_streamId(), this.eventTypes);
        }
        this.epService = EPServiceProviderManager.getDefaultProvider(cepConfig);
        this.epService.initialize();
        if (!processStatemens()) {
            throw new FailedException("At least one type of statement has to be not empty");
        }
    }

    private boolean processStatemens() {
        boolean hasStatemens = false;
        if (this.statements != null) {
            for (String s : this.statements) {
                this.epService.getEPAdministrator().createEPL(s).addListener(this);
                hasStatemens = true;
            }
        }
        if (this.objectStatements != null) {
            for (EPStatementObjectModel s : this.objectStatements) {
                this.epService.getEPAdministrator().create(s).addListener(this);
                hasStatemens = true;
            }
        }
        return hasStatemens;
    }

    @Override
    public void execute(Tuple tuple) {
        Map<String, Object> tuplesper = new HashMap<>();
        for (String f : tuple.getFields()) {
            tuplesper.put(f, tuple.getValueByField(f));
        }
        this.epService.getEPRuntime().sendEvent(tuplesper, tuple.getSourceComponent() + "_" + tuple.getSourceStreamId());
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        if (this.epService != null) {
            this.epService.destroy();
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        if (newEvents == null) {
            return;
        }
        for (EventBean newEvent : newEvents) {
            List<Object> tuple = new ArrayList<>();
            String eventType;
            if (outputTypes.containsKey(newEvent.getEventType().getName())) {
                eventType = newEvent.getEventType().getName();
                for (String field : outputTypes.get(newEvent.getEventType().getName())) {
                    if (newEvent.get(field) != null) {
                        tuple.add(newEvent.get(field));
                    }
                }
            } else {
                eventType = "default";
                for (String field : newEvent.getEventType().getPropertyNames()) {
                    tuple.add(newEvent.get(field));
                }
            }
            collector.emit(eventType, tuple);
        }
    }

}
