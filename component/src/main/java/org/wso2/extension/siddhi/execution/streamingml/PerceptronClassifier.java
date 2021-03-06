package org.wso2.extension.siddhi.execution.streamingml;
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

import java.io.Serializable;

/**
 * Implementation of the Perceptron Classifier algorithm
 */
public class PerceptronClassifier implements Serializable {

    //weights list
    private double[] weights;

    //parameters for learning
    private double bias = 0.0;
    private double threshold = 0.5;
    private double learningRate = 0.1;

    public  PerceptronClassifier(){};

    public PerceptronClassifier(double bias, double threshold, double learningRate) {
        this.bias = bias;
        this.threshold = threshold;
        this.learningRate = learningRate;
    }

    /**
     * Perform the learning of the Perceptron when single data point
     *
     * @param label - label used to train
     * @param features - feature set containing data to train
     */
    public void update(Boolean label, double[] features) {
        Boolean prediction = this.classify(features);

        if (!label.equals(prediction)) {
            double error =-1;
            if (Boolean.TRUE.equals(label)){
                error = 1.0;
            }

            for (int i = 0; i < features.length; i++) {
                weights[i] = weights[i] + (features[i] * error * learningRate);
            }
        }
    }

    /**
     * Perform the classification using the Perceptron model
     *
     * @param features - data to perform the classification/prediction
     * @return
     */
    public Boolean classify(double[] features) {
        if (this.weights == null) {
            initWeights(features.length);
        }
        Double evaluation = dotMultiplication(features, weights) + this.bias;
        Boolean prediction = Boolean.FALSE;
        if(evaluation > threshold){
            prediction = Boolean.TRUE;
        }
        return prediction;
    }

    /**
     * Initialize the weight vector
     * @param size - size of the weight vector
     */
    private void initWeights(int size) {
        weights = new double[size];
    }

    /**
     * Performs vector dot multiplication
     * @param vector1 - first vector to multiply
     * @param vector2 - This is multiplied with the
     * @return
     */
    private double dotMultiplication(double[] vector1, double[] vector2) {
        if (vector1.length != vector2.length) {
            throw new IllegalArgumentException("The dimensions have to be equal");
        }
        double sum = 0;
        for (int i = 0; i < vector1.length; i++) {
            sum += vector1[i] * vector2[i];
        }
        return sum;
    }

}
