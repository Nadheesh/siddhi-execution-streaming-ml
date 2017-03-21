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
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implement hte Streaming ML Stream processor Siddhi extension
 */
public class StreamingMLStreamProcessor extends StreamProcessor{

    private String modelType;
    private PerceptronClassifier model; //model to train (currently only Perceptrons)
    private Map<Integer, int[]> attributeIndexMap;
    private boolean training;

    /**
     * Processed values in the each event
     * @param complexEventChunk
     * @param processor
     * @param streamEventCloner
     * @param complexEventPopulater
     */
    @Override protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (complexEventChunk.hasNext()){
            StreamEvent event = complexEventChunk.next();
            String[] featureValues = new String[attributeIndexMap.size()];

            //extract values from features
            for (Map.Entry<Integer, int[]> entry : attributeIndexMap.entrySet()) {
                int featureIndex = entry.getKey();
                int[] attributeIndexArray = entry.getValue();
                Object dataValue = null;
                switch (attributeIndexArray[2]) {
                case 0:
                    dataValue = event.getBeforeWindowData()[attributeIndexArray[3]];
                    break;
                case 2:
                    dataValue = event.getOutputData()[attributeIndexArray[3]];
                    break;
                }
                featureValues[featureIndex] = String.valueOf(dataValue);
            }

            if(featureValues!=null) {
                //set the training label
                Boolean label;
                if ("true".equals(featureValues[0].toLowerCase())){
                    label = Boolean.TRUE;
                }else if("false".equals(featureValues[0].toLowerCase())){
                    label = Boolean.FALSE;
                }else {
                    throw new ExecutionPlanRuntimeException(
                            "Label should be \'true\' or \'false\'");
                }

                double[] features = new double[featureValues.length - 1];

                //parsing the values from the features to double
                try {
                    for (int i = 1; i < featureValues.length; i++) {
                        features[i - 1] = Double.parseDouble(featureValues[i]);
                    }
                }catch (NumberFormatException ex){
                    throw new ExecutionPlanRuntimeException(
                            "Feature values should be numeric");
                }

                //update the model
                if (features.length>0) {

                    Boolean prediction = model.classify(features);
                    model.update(label, features);

                    Object[] output = new Object[] {prediction};
                    complexEventPopulater.populateComplexEvent(event, output);

                }else {
                    throw new ExecutionPlanRuntimeException(
                            "No feature values found");
                }
            }
        }
        nextProcessor.process(complexEventChunk);
    }

    /**
     * Initiating the query
     * @param abstractDefinition
     * @param expressionExecutors
     * @param executionPlanContext
     * @return
     */
    @Override protected List<Attribute> init(AbstractDefinition abstractDefinition,
            ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        //if training
        training = true;

        if (attributeExpressionExecutors.length < 3){
            throw new ExecutionPlanValidationException(
                    "Model Type or feature attributes are not defined");
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
            modelType = (String) constantObj;
        } else {
            throw new ExecutionPlanValidationException(
                    "Model Type is not provided as the first parameter");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor || !attributeExpressionExecutors[1].getReturnType().equals(Attribute.Type.BOOL)) {
            throw new ExecutionPlanValidationException(
                    "Second parameter should be the label attribute with type boolean");
        }

        if("perceptron".equals(modelType.toLowerCase())){
            model= new PerceptronClassifier();
            //return new ArrayList<Attribute>();
            return Arrays.asList(new Attribute("prediction", Attribute.Type.BOOL));
        }else {
            throw new ExecutionPlanValidationException(
                    "Undefined model type");
        }
    }

    //create Feature and attribute map
    @Override public void start() {
        if (training){
            setFeatureAttributeMapping();
        }
    }

    private void setFeatureAttributeMapping(){
        attributeIndexMap = new HashMap<Integer, int[]>();
        int featureIndex = 0;
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            if (expressionExecutor instanceof VariableExpressionExecutor) {
                VariableExpressionExecutor variable = (VariableExpressionExecutor) expressionExecutor;
                attributeIndexMap.put(featureIndex, variable.getPosition());
                featureIndex++;
            }
        }
    }

    @Override public void stop() {}

    @Override public Object[] currentState() {
        return new Object[0];
    }

    @Override public void restoreState(Object[] objects) {

    }
}
