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
package com.ibm.eventstreams.connect.mqsource;

import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.putAllMessagesToQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MessagesObjectMother.createAListOfMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class JMSWorkerIT extends AbstractJMSContextIT {
    private JMSWorker jmsWorker;

    @Before
    public void setUpJMSWorker() {
        Map<String, String> defaultConnectorProperties = getDefaultConnectorProperties();
        defaultConnectorProperties.put("mq.message.body.jms", "true");
        defaultConnectorProperties.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        jmsWorker = new JMSWorker();
        jmsWorker.configure(getPropertiesConfig(defaultConnectorProperties));
        jmsWorker.connect();
    }

    @After
    public void cleanStateQueue() {
        try {
            MQTestUtil.removeAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
            jmsWorker.stop();
        } catch (JMSException e) {
            System.out.println("Could not remove message from queue");
        }
    }

    @Test
    public void testToSourceRecord() throws Exception {

        TextMessage textMessage = getJmsContext().createTextMessage("This is my test message");

        // Map<String, Long> sourceOffset = null;
        Map<String, Long> sourceOffset = new HashMap<>();
        sourceOffset.put("sequence-id", 5L);

        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("source", "myqmgr/myq");

        SourceRecord sourceRecord = jmsWorker.toSourceRecord(textMessage, true, sourceOffset, sourcePartition);

        assertThat(sourceRecord).isNotNull();
        assertThat(sourceRecord.sourceOffset()).isEqualTo(sourceOffset);
        assertThat(sourceRecord.sourcePartition()).isEqualTo(sourcePartition);

        // verify the Kafka sourceRecord
        assertNull(sourceRecord.key());
        assertEquals(textMessage.getText(), sourceRecord.value());
        assertNull(sourceRecord.valueSchema());
    }

    @Test
    public void testQueueHoldsMoreThanOneMessage_zeroMessagesOnQueue() throws JMSException {
        assertThat(jmsWorker.queueHoldsMoreThanOneMessage(DEFAULT_STATE_QUEUE)).isFalse();
    }

    @Test
    public void testQueueHoldsMoreThanOneMessage_oneMessageOnQueue() throws Exception {
        final List<Message> messages = createAListOfMessages(getJmsContext(), 1, "test message ");
        putAllMessagesToQueue(DEFAULT_STATE_QUEUE, messages);
        assertThat(jmsWorker.queueHoldsMoreThanOneMessage(DEFAULT_STATE_QUEUE)).isFalse();
    }

    @Test
    public void testQueueHoldsMoreThanOneMessage_twoMessageOnQueue() throws Exception {
        final List<Message> messages = createAListOfMessages(getJmsContext(), 2, "test message ");
        putAllMessagesToQueue(DEFAULT_STATE_QUEUE, messages);
        assertThat(jmsWorker.queueHoldsMoreThanOneMessage(DEFAULT_STATE_QUEUE)).isTrue();
    }

    @Test
    public void testQueueHoldsMoreThanOneMessage_queueNotFound() {
        assertThrows(Exception.class, ()->jmsWorker.queueHoldsMoreThanOneMessage("QUEUE_DOES_NOT_EXIST"));
    }

}