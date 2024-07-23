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
package com.ibm.eventstreams.connect.mqsource.sequencestate;

import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.CHANNEL_NAME;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.DEFAULT_CONNECTION_NAME;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.DEFAULT_SOURCE_QUEUE;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.DEFAULT_STATE_QUEUE;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.QMGR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.jms.JMSException;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.eventstreams.connect.mqsource.JMSWorker;

public class SequenceStateClientTest {

    private Map<String, String> getDefaultConnectorProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", QMGR_NAME);
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", DEFAULT_CONNECTION_NAME);
        props.put("mq.channel.name", CHANNEL_NAME);
        props.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        props.put("mq.user.authentication.mqcsp", "false");
        return props;
    }

    @Test
    public void test_emptyResponseFromKafkaOffsetTopic_ThenOptionalEmpty() {

        Map<String, String> props = getDefaultConnectorProperties();
        props.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        SequenceStateClient sequenceStateClient = new SequenceStateClient(
                DEFAULT_STATE_QUEUE,
                Mockito.mock(JMSWorker.class),
                Mockito.mock(JMSWorker.class)
        );

        SourceTaskContext contextMock = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReaderMock = Mockito.mock(OffsetStorageReader.class);
        Mockito.when(offsetStorageReaderMock.offset(any())).thenReturn(Collections.emptyMap());
        Mockito.when(contextMock.offsetStorageReader()).thenReturn(offsetStorageReaderMock);

        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("source", "myqmgr/myq");

        Optional<Long> sequenceFromKafkaOffset = sequenceStateClient.getSequenceFromKafkaOffset(contextMock, "test-offset", sourcePartition);

        assertThat(sequenceFromKafkaOffset).isEmpty();
    }

    @Test
    public void test_NullResponseFromKafkaOffsetTopic_ThenOptionalEmpty() {

        Map<String, String> props = getDefaultConnectorProperties();
        props.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        SequenceStateClient sequenceStateClient = new SequenceStateClient(
                DEFAULT_STATE_QUEUE,
                Mockito.mock(JMSWorker.class),
                Mockito.mock(JMSWorker.class)
        );

        SourceTaskContext contextMock = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReaderMock = Mockito.mock(OffsetStorageReader.class);
        Mockito.when(offsetStorageReaderMock.offset(any())).thenReturn(null);
        Mockito.when(contextMock.offsetStorageReader()).thenReturn(offsetStorageReaderMock);

        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("source", "myqmgr/myq");

        Optional<Long> sequenceFromKafkaOffset = sequenceStateClient.getSequenceFromKafkaOffset(contextMock, "test-offset", sourcePartition);

        assertThat(sequenceFromKafkaOffset).isEmpty();
    }

    @Test(expected = Test.None.class /* no exception expected */)
    public void test_validateStateQueue_OK() throws JMSException {
        Map<String, String> props = getDefaultConnectorProperties();
        props.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        JMSWorker dedicatedMock = Mockito.mock(JMSWorker.class);
        Mockito.when(dedicatedMock.queueHoldsMoreThanOneMessage(anyString())).thenReturn(false);

        SequenceStateClient sequenceStateClient = new SequenceStateClient(
                DEFAULT_STATE_QUEUE,
                Mockito.mock(JMSWorker.class),
                dedicatedMock
        );

        sequenceStateClient.validateStateQueue();
    }

    @Test
    public void test_validateStateQueue_NOT_OK() throws JMSException {
        Map<String, String> props = getDefaultConnectorProperties();
        props.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        JMSWorker dedicatedMock = Mockito.mock(JMSWorker.class);
        Mockito.when(dedicatedMock.queueHoldsMoreThanOneMessage(anyString())).thenReturn(true);

        SequenceStateClient sequenceStateClient = new SequenceStateClient(
                DEFAULT_STATE_QUEUE,
                Mockito.mock(JMSWorker.class),
                dedicatedMock
        );

        Exception exception = assertThrows(Exception.class, sequenceStateClient::validateStateQueue);
        String actualMessage = exception.getMessage();

        assertTrue(actualMessage.contains("more than one message"));
    }
}