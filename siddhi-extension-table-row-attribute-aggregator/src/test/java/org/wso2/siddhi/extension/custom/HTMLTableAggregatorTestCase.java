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

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class HTMLTableAggregatorTestCase {
    private static final Logger log = Logger.getLogger(TableRowAttributeAggregatorTestCase.class);
    private volatile int count;

    @Before
    public void init() {
        count = 0;
    }

    @Test
    public void tableRowMultiAttributeTest() throws InterruptedException {
        log.info("HTML Table Aggregator TestCase - Multi");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (id int, timestamp long, value int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#window.time(2 sec) " +
                "select id, html:toTable(id, timestamp, value) as rowString " +
                "insert into outputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (org.wso2.siddhi.core.event.Event event : events) {
                    count++;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{1, 1470732420000L, 0});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2, 1470732423000L, 86});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3, 1470732424000L, 87});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{4, 1470732425000L, 88});
        inputHandler.send(new Object[]{5, 1470732426000L, 84});
        Thread.sleep(3000);
        inputHandler.send(new Object[]{6, 1470732427000L, 0});
        Thread.sleep(1000);
        executionPlanRuntime.shutdown();
        Assert.assertEquals("Event count", 6, count);
    }

    @Test
    public void tableRowSingleAttributeTest() throws InterruptedException {
        log.info("HTML Table Aggregator TestCase  - Single");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (id int, timestamp long, value int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#window.time(2 sec) " +
                "select id, html:toTable(value) as rowString " +
                "insert into outputStream; ";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                EventPrinter.print(events);
                for (org.wso2.siddhi.core.event.Event event : events) {
                    count++;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{1, 1470732420000L, 0});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{2, 1470732423000L, 86});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{3, 1470732424000L, 87});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{4, 1470732425000L, 88});
        inputHandler.send(new Object[]{5, 1470732426000L, 84});
        Thread.sleep(3000);
        inputHandler.send(new Object[]{6, 1470732427000L, 0});
        Thread.sleep(1000);
        executionPlanRuntime.shutdown();
        Assert.assertEquals("Event count", 6, count);
    }
}