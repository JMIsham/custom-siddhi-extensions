/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.text.MessageFormat;

public class TableRowAttributeAggregator extends AttributeAggregator {
    private StringBuilder sb = new StringBuilder();
    private String template = null;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length == 0) {
            throw new OperationNotSupportedException("Table row aggregator has to have at least 1 parameter, " +
                    "currently " + attributeExpressionExecutors.length + " parameters provided");
        }
        template = generateTemplate(attributeExpressionExecutors.length);
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    @Override
    public Object processAdd(Object data) {
        sb.append(map(new Object[]{data}));
        return sb.toString();
    }

    @Override
    public Object processAdd(Object[] data) {
        sb.append(map(data));
        return sb.toString();
    }

    @Override
    public Object processRemove(Object data) {
        return "";
    }

    @Override
    public Object processRemove(Object[] data) {
        return "";
    }

    @Override
    public Object reset() {
        sb.setLength(0);
        sb.trimToSize();
        return "";
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //Nothing to stop
    }

    @Override
    public Object[] currentState() {
        return new Object[]{sb, template};
    }

    @Override
    public void restoreState(Object[] state) {
        sb = (StringBuilder) state[0];
        template = (String) state[1];
    }

    private String generateTemplate(int attributeCount) {
        StringBuilder builder = new StringBuilder().append("<tr>");
        for (int i = 0; i < attributeCount; i++) {
            builder.append(String.format("<td>{%s}</td>", i));
        }
        builder.append("</tr>");
        return builder.toString();
    }

    private String map(Object[] data) {
        return MessageFormat.format(template, data);
    }

}
