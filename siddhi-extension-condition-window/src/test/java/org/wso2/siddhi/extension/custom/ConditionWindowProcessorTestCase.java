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

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

public class ConditionWindowProcessorTestCase {
    private static final Logger log = Logger.getLogger(ConditionWindowProcessorTestCase.class);
    private volatile int count;

    @Before
    public void init() {
        count = 0;
    }

    @Test
    public void conditionWindowTest() throws InterruptedException {
        log.info("Distinct Count TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "" +
                "define stream inputStream (id int, timestamp long, value int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from inputStream#window.custom:conditionWindow(value >= 85, 3) " +
                "select id, value " +
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
        inputHandler.send(new Object[]{2, 1470732421000L, 0});
        inputHandler.send(new Object[]{3, 1470732422000L, 0});
        inputHandler.send(new Object[]{4, 1470732423000L, 86});
        inputHandler.send(new Object[]{5, 1470732424000L, 87});
        inputHandler.send(new Object[]{6, 1470732425000L, 88});
        inputHandler.send(new Object[]{7, 1470732426000L, 84});
        inputHandler.send(new Object[]{8, 1470732427000L, 0});
        inputHandler.send(new Object[]{9, 1470732428000L, 0});
        inputHandler.send(new Object[]{10, 1470732429000L, 0});
        inputHandler.send(new Object[]{11, 1470732430000L, 89});
        inputHandler.send(new Object[]{12, 1470732431000L, 89});
        inputHandler.send(new Object[]{13, 1470732432000L, 87});
        inputHandler.send(new Object[]{14, 1470732433000L, 89});
        inputHandler.send(new Object[]{15, 1470732434000L, 85});
        inputHandler.send(new Object[]{16, 1470732435000L, 89});
        inputHandler.send(new Object[]{17, 1470732436000L, 89});
        inputHandler.send(new Object[]{18, 1470732437000L, 87});
        inputHandler.send(new Object[]{19, 1470732438000L, 86});
        inputHandler.send(new Object[]{20, 1470732439000L, 88});
        inputHandler.send(new Object[]{21, 1470732440000L, 0});
        inputHandler.send(new Object[]{22, 1470732441000L, 0});
        inputHandler.send(new Object[]{23, 1470732442000L, 0});
        inputHandler.send(new Object[]{24, 1470732443000L, 87});
        inputHandler.send(new Object[]{25, 1470732444000L, 85});
        inputHandler.send(new Object[]{26, 1470732445000L, 86});
        inputHandler.send(new Object[]{27, 1470732446000L, 0});
        inputHandler.send(new Object[]{28, 1470732447000L, 0});
        inputHandler.send(new Object[]{29, 1470732448000L, 0});
        inputHandler.send(new Object[]{30, 1470732449000L, 0});
        Thread.sleep(1000);
        executionPlanRuntime.shutdown();
        Assert.assertEquals("Event count", 3, count);
    }
}