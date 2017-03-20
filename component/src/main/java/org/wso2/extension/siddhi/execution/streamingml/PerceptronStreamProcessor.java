
/*
*  Copyright (c) 3/19/2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.extension.siddhi.execution.streamingml;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

public class PerceptronStreamProcessor extends StreamProcessor{

    private String modelType;
    private PerceptronClassifier model;
    @Override protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (complexEventChunk.hasNext()){
            StreamEvent event = complexEventChunk.next();
        }
    }

    @Override protected List<Attribute> init(AbstractDefinition abstractDefinition,
            ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
            modelType = (String) constantObj;
        } else {
            throw new ExecutionPlanValidationException(
                    "Model Type is not provided as the first parameter");
        }
        if("Perceptron".equals(modelType)){
            model= new PerceptronClassifier();
            return new ArrayList<Attribute>();

        }else {
            throw new ExecutionPlanValidationException(
                    "Undefined model type");
        }

    }

    @Override public void start() {

    }

    @Override public void stop() {

    }

    @Override public Object[] currentState() {
        return new Object[0];
    }

    @Override public void restoreState(Object[] objects) {

    }
}
