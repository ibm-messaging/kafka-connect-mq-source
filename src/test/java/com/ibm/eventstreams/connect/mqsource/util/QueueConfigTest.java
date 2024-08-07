/**
 * Copyright 2023, 2024 IBM Corporation
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

import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.jms.MQQueue;
import com.ibm.msg.client.wmq.WMQConstants;
import junit.framework.TestCase;
import org.mockito.Mockito;
import javax.jms.JMSException;
import static org.assertj.core.api.Assertions.assertThat;


public class QueueConfigTest extends TestCase {

    public void testApplyToQueue_JMSBody() throws JMSException {
        QueueConfig jmsBodyConfig = new QueueConfig(true, false);
        MQQueue mockQueue = Mockito.mock(MQQueue.class);

        jmsBodyConfig.applyToQueue(mockQueue);

        Mockito.verify(mockQueue).setMessageBodyStyle(MQConstants.WMQ_MESSAGE_BODY_JMS);
    }

    public void testApplyToQueue_MQBody() throws JMSException {
        QueueConfig mqBodyConfig = new QueueConfig(false, false);
        MQQueue mockQueue = Mockito.mock(MQQueue.class);

        mqBodyConfig.applyToQueue(mockQueue);

        Mockito.verify(mockQueue).setMessageBodyStyle(MQConstants.WMQ_MESSAGE_BODY_MQ);
    }

    public void testApplyToQueue_MQMDRead() throws JMSException {
        QueueConfig mqmdReadBodyConfig = new QueueConfig(false, true);
        MQQueue mockQueue = Mockito.mock(MQQueue.class);
        mqmdReadBodyConfig.applyToQueue(mockQueue);

        Mockito.verify(mockQueue).setBooleanProperty(WMQConstants.WMQ_MQMD_READ_ENABLED,true);

        QueueConfig mqmdNoReadBodyConfig = new QueueConfig(false, false);
        MQQueue mockQueue2 = Mockito.mock(MQQueue.class);
        mqmdNoReadBodyConfig.applyToQueue(mockQueue2);

        Mockito.verify(mockQueue2, Mockito.never()).setBooleanProperty(WMQConstants.WMQ_MQMD_READ_ENABLED,true);
    }

    public void testIsMqMessageBodyJms() {
        QueueConfig jmsBodyConfig = new QueueConfig(true, false);
        assertThat(jmsBodyConfig.isMqMessageBodyJms()).isTrue();

        QueueConfig mqBodyConfig = new QueueConfig(false, false);
        assertThat(mqBodyConfig.isMqMessageBodyJms()).isFalse();
    }

    public void testIsMqMessageMqmdRead() {
        QueueConfig mqmdBodyConfig = new QueueConfig(false, true);
        assertThat(mqmdBodyConfig.isMqMessageMqmdRead()).isTrue();

        QueueConfig nomqmdBodyConfig = new QueueConfig(false, false);
        assertThat(nomqmdBodyConfig.isMqMessageMqmdRead()).isFalse();

    }
}