package org.wso2.extension.siddhi.execution.streamingml;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.concurrent.atomic.AtomicInteger;

/*
*  Copyright (c) 3/21/2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
public class StreamingMLStreamProcessorTest {
    static final Logger logger = Logger.getLogger(StreamingMLStreamProcessorTest.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test public void testStreamingMLStreamProcessorExtension() throws InterruptedException {
        logger.info("StreamingMLStreamProcessor TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream(label bool, f1 double, f2 double, f3 double);";
        String query = ("@info(name = 'query1') " + "from inputStream#sml:pml('perceptron', label, f1, f2, f3)"
                                + " select *" + " insert into outputStream;");
        try {
            siddhiManager.validateExecutionPlan(inStreamDefinition + query);
        } catch (ExecutionPlanValidationException ex) {
            logger.error(ex.getMessage(), ex);
        }
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(false, event.getData(4));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, event.getData(4));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(false, event.getData(4));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[] { true, 223, 12, 121.2 });
        inputHandler.send(new Object[] { false, 12.1, 2123.12, 999 });
        inputHandler.send(new Object[] { true, 11.1, 123.1, 91.2 });
        sleepTillArrive(4001);
        Assert.assertEquals(3, count.get());
        Assert.assertTrue(eventArrived
        );
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();

    }

    private void sleepTillArrive(int milliseconds) {
        int totalTime = 0;
        while (!eventArrived && totalTime < milliseconds) {
            int t = 1000;
            try {
                Thread.sleep(t);
            } catch (InterruptedException ignore) {
            }
            totalTime += t;
        }
    }
}