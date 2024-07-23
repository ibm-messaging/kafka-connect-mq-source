/**
 * Copyright 2023 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsource.util;

import org.apache.kafka.connect.errors.ConnectException;

import com.ibm.mq.MQException;

import junit.framework.TestCase;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

public class ExceptionProcessorTest extends TestCase {
    
    public void test_getReasonWithNonMQException() {
        ConnectException exp = new ConnectException("test text");
        int reason = ExceptionProcessor.getReason(exp);
        assertThat(reason).isEqualTo(-1);
    }

    public void test_getReasonWithMQException() {
        MQException exp = new MQException(1, 1, getClass());
        MQException wrapper_exp = new MQException(1, 1, exp, exp);
        int reason = ExceptionProcessor.getReason(wrapper_exp);
        assertThat(reason).isGreaterThan(-1);
    }

    public void test_isClosableWithMQExceptionErrorNotClosable() {
        MQException exp = new MQException(1, 1, getClass());
        MQException wrapper_exp = new MQException(1, 1, exp, exp);
        boolean isClosable = ExceptionProcessor.isClosable(wrapper_exp);
        assertThat(isClosable).isTrue();
    }

    public void test_isClosableWithMQExceptionErrorIsClosable() {
        MQException exp = new MQException(1, 2016, getClass());
        MQException wrapper_exp = new MQException(1, 1, exp, exp);
        boolean isClosable = ExceptionProcessor.isClosable(wrapper_exp);
        assertThat(isClosable).isFalse();
    }

    public void test_isRetriableWithMQExceptionErrorsAreRetriable() {
        final List<Integer> reasonsRetriable = new ArrayList<>();
        reasonsRetriable.add(2003);
        reasonsRetriable.add(2537);
        reasonsRetriable.add(2009);
        reasonsRetriable.add(2538);
        reasonsRetriable.add(2035);        
        reasonsRetriable.add(2059);
        reasonsRetriable.add(2161);
        reasonsRetriable.add(2162);
        reasonsRetriable.add(2195);
        reasonsRetriable.add(2016);
        for (int reason : reasonsRetriable){
            createAndProcessExceptionThrough_isRetriable_andAssert(reason, true);
        }
    }

    public void test_isRetriableWithMQExceptionErrorsAreNotRetriable() {
        createAndProcessExceptionThrough_isRetriable_andAssert(1, false);
    }

    private void createAndProcessExceptionThrough_isRetriable_andAssert(int reason, Boolean expectedResult) {
        MQException exp = new MQException(1, reason, getClass());
        MQException wrapper_exp = new MQException(1, 1, exp, exp);
        assertThat(ExceptionProcessor.isRetriable(wrapper_exp)).isEqualTo(expectedResult);
    }
}
