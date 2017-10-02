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
import java.util.ArrayList;
import java.util.List;

public class HTMLTableAggregator extends AttributeAggregator {
    private StringBuilder sb = new StringBuilder();
    private String template = null;
    private static final String TABLE_ROWS = "TABLE_ROWS";
    private List<String> rows;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length == 0) {
            throw new OperationNotSupportedException("HTML table aggregator has to have at least 1 parameter, " +
                    "currently " + attributeExpressionExecutors.length + " parameters provided");
        }
        rows = new ArrayList<String>();
    }

    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    @Override
    public Object processAdd(Object data) {
        String tableRow = "<tr> <td>" + data + "</td></tr>";
        rows.add(tableRow);
        return getHTMLTable(rows);
    }

    @Override
    public Object processAdd(Object[] data) {
        int i;
        String tableRow = "<tr>";
        for (i = 0; i < data.length; i++) {
            tableRow = tableRow + "<td>" + data[i] + "</td>";
        }
        tableRow = tableRow + "</tr>";
        rows.add(tableRow);
        return getHTMLTable(rows);
    }

    @Override
    public Object processRemove(Object data) {
        String tableRow = "<tr> <td>" + data + "</td></tr>";
        rows.remove(tableRow);
        return getHTMLTable(rows);
    }

    @Override
    public Object processRemove(Object[] data) {
        int i;
        String tableRow = "<tr>";
        for (i = 0; i < data.length; i++) {
            tableRow = tableRow + "<td>" + data[i] + "</td>";
        }
        tableRow = tableRow + "</tr>";
        rows.remove(tableRow);
        return getHTMLTable(rows);
    }

    @Override
    public Object reset() {
        rows.clear();
        return "<tbody></tbody>";
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

    private String getHTMLTable(List<String> list) {
        String table = "<tbody>";
        int i;
        for (i = 0; i < list.size(); i++) {
            table = table + list.get(i);
        }
        table = table + "</tbody>";
        return table;

    }

    private String map(Object[] data) {
        return MessageFormat.format(template, data);
    }

}
