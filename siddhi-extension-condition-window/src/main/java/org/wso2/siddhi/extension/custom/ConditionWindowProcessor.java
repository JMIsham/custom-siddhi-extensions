/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.custom;


import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.compare.CompareConditionExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class ConditionWindowProcessor extends WindowProcessor implements FindableProcessor {
    private CopyOnWriteArrayList<StreamEvent> currentEvents = new CopyOnWriteArrayList<StreamEvent>();
    private ComplexEventChunk<StreamEvent> eventChunk = new ComplexEventChunk<StreamEvent>(false);
    private CompareConditionExpressionExecutor conditionExpressionExecutor;
    private int consecutiveEventCount;
    private boolean batchProcessed = false;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length == 2) {
            if (!(attributeExpressionExecutors[0] instanceof CompareConditionExpressionExecutor)) {
                throw new ExecutionPlanValidationException("Condition window's 1st parameter should be a condition, " +
                        "but found " + attributeExpressionExecutors[0].getClass());
            }
            conditionExpressionExecutor = (CompareConditionExpressionExecutor) attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                consecutiveEventCount = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new ExecutionPlanValidationException("Condition window's 2nd parameter consecutiveEventCount " +
                        "should be an int, but found " + attributeExpressionExecutors[1].getReturnType());
            }
        } else {
            throw new ExecutionPlanValidationException("Condition window should only 2 parameters (<condition> " +
                    "condition, <int> consecutiveEventCount), but found " + attributeExpressionExecutors.length +
                    " input attributes");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk,
                           Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                clonedEvent.setType(StreamEvent.Type.CURRENT);

                if (conditionExpressionExecutor.execute(clonedEvent)) {
                    if (!batchProcessed) {
                        currentEvents.add(clonedEvent);
                        if (currentEvents.size() == consecutiveEventCount) {
                            batchProcessed = true;
                            eventChunk.clear();
                            eventChunk.add(currentEvents.get(consecutiveEventCount - 1));
                            eventChunk.reset();
                            currentEvents.clear();
                            nextProcessor.process(eventChunk);
                        }
                    }
                } else {
                    batchProcessed = false;
                    currentEvents.clear();
                }
            }
        }
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{currentEvents};
    }

    @Override
    public void restoreState(Object[] state) {
        currentEvents = (CopyOnWriteArrayList<StreamEvent>) state[0];
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, currentEvents, streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression,
                                  MatchingMetaStateHolder matchingMetaStateHolder,
                                  ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap) {
        return OperatorParser.constructOperator(currentEvents, expression, matchingMetaStateHolder,
                executionPlanContext, variableExpressionExecutors, eventTableMap);
    }
}
