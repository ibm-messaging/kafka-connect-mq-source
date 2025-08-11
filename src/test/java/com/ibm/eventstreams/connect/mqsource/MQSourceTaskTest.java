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
package com.ibm.eventstreams.connect.mqsource;

import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceState;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateClient;
import com.ibm.eventstreams.connect.mqsource.util.QueueConfig;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.TextMessage;

import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.CHANNEL_NAME;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.DEFAULT_CONNECTION_NAME;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.DEFAULT_SOURCE_QUEUE;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.DEFAULT_STATE_QUEUE;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.QMGR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class MQSourceTaskTest {

    @Mock private TextMessage jmsMessage;
    @Mock private JMSWorker jmsWorker;
    @Mock private JMSWorker dedicatedWorker;
    @Mock private SequenceStateClient sequenceStateClient;
    @Mock private SourceTaskContext sourceTaskContext;

    private final static int MQ_BATCH_SIZE = 8;
    private final static int MAX_POLL_BLOCKED_TIME_MS = 100;

    private Map<String, String> createDefaultConnectorProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", QMGR_NAME);
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", DEFAULT_CONNECTION_NAME);
        props.put("mq.channel.name", CHANNEL_NAME);
        props.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        props.put("topic", "mytopic");
        props.put("mq.batch.size", Integer.toString(MQ_BATCH_SIZE));
        props.put("mq.max.poll.blocked.time.ms", Integer.toString(MAX_POLL_BLOCKED_TIME_MS));
        return props;
    }

    private Map<String, String> createExactlyOnceConnectorProperties() {
        final Map<String, String> props = createDefaultConnectorProperties();
        props.put("mq.exactly.once.state.queue", DEFAULT_STATE_QUEUE);
        props.put("tasks.max", "1");
        return props;
    }

    @Test
    public void testSequenceStateClientBrowseHasBeenCalledInStart() throws JMSRuntimeException, JMSException {

        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        mqSourceTask.start(createExactlyOnceConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        Mockito.verify(sequenceStateClient, Mockito.times(1)).browse();
    }

    @Test
    public void testStartWhenAtLeastOnceDeliveryConfig() throws JMSRuntimeException, JMSException {

        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        mqSourceTask.start(createDefaultConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        // Check that sequenceStateClient is not called and the the startup action is normal
        Mockito.verify(sequenceStateClient, Mockito.times(0)).browse();
        assertThat(mqSourceTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.NORMAL_OPERATION);
        assertThat(mqSourceTask.getSequenceId().get()).isEqualTo(0L);
    }

    @Test
    public void testStartWhenNoPrepareMessageInMQAndNoKafkaOffset() {

        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        Mockito.when(sequenceStateClient.getSequenceFromKafkaOffset(any(SourceTaskContext.class), anyString(), anyMap())).thenReturn(Optional.of(5L));
        mqSourceTask.start(createExactlyOnceConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        assertThat(mqSourceTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.NORMAL_OPERATION);

        assertThat(mqSourceTask.getSequenceId().get()).isEqualTo(5L);
    }

    @Test
    public void testStartWithKafkaOffsetButNoMQOffset() {

        MQSourceTask mqSourceTask = new MQSourceTask();
        // setUpEmptyKafkaOffsetMock();
        mqSourceTask.initialize(sourceTaskContext);
        mqSourceTask.start(createExactlyOnceConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        assertThat(mqSourceTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.NORMAL_OPERATION);

        assertThat(mqSourceTask.getSequenceId().get()).isEqualTo(0L);
    }

    @Test
    public void testStartWhenPrepareMessageInMQAndSequenceStateIsDELIVERED() throws JMSRuntimeException, JMSException {
        List<String> messageIds = Arrays.asList("1", "2", "3", "4", "5");

        Mockito.when(sequenceStateClient.browse()).thenReturn(
            Optional.of(new SequenceState(
                    5,
                    messageIds,
                    SequenceState.LastKnownState.DELIVERED))
        );
        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        mqSourceTask.start(createExactlyOnceConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);
        assertThat(mqSourceTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE);

        assertThat(mqSourceTask.getSequenceId().get()).isEqualTo(5L);
        assertThat(mqSourceTask.getMsgIds()).isEqualTo(messageIds);
    }

    @Test
    public void testStartWhenPrepareMessageInMQAndSequenceStateIsIN_FLIGHTNoKafkaState() throws JMSRuntimeException, JMSException {
        List<String> messageIds = Arrays.asList("1", "2", "3", "4", "5");

        Mockito.when(sequenceStateClient.browse()).thenReturn(
            Optional.of(new SequenceState(
                    6,
                    messageIds,
                    SequenceState.LastKnownState.IN_FLIGHT))
        );
        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        Mockito.when(sequenceStateClient.getSequenceFromKafkaOffset(any(SourceTaskContext.class), anyString(), anyMap())).thenReturn(Optional.empty());
        mqSourceTask.start(createExactlyOnceConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        assertThat(mqSourceTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REDELIVER_UNSENT_BATCH);

        assertThat(mqSourceTask.getSequenceId().get()).isEqualTo(6L);
        assertThat(mqSourceTask.getMsgIds()).isEqualTo(messageIds);
    }

    @Test
    public void testStartWhenPrepareMessageInMQAndSequenceStateIsIN_FLIGHTWithKafkaStateUnmatched() throws JMSRuntimeException, JMSException {
        List<String> messageIds = Arrays.asList("1", "2", "3", "4", "5");

        Mockito.when(sequenceStateClient.browse()).thenReturn(
            Optional.of(new SequenceState(
                    2L,
                    messageIds,
                    SequenceState.LastKnownState.IN_FLIGHT))
        );
        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        Mockito.when(sequenceStateClient.getSequenceFromKafkaOffset(any(SourceTaskContext.class), anyString(), anyMap())).thenReturn(Optional.of(1L));
        mqSourceTask.start(createExactlyOnceConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        assertThat(mqSourceTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REDELIVER_UNSENT_BATCH);

        assertThat(mqSourceTask.getSequenceId().get()).isEqualTo(2L);
        assertThat(mqSourceTask.getMsgIds()).isEqualTo(messageIds);
    }

    @Test
    public void testStart_WhenPrepareMessageInMQ_AndSequenceStateIsIN_FLIGHTWithKafkaStateMatched() throws JMSRuntimeException, JMSException {
        List<String> messageIds = Arrays.asList("1", "2", "3", "4", "5");

        Mockito.when(sequenceStateClient.browse()).thenReturn(
            Optional.of(new SequenceState(
                    7,
                    messageIds,
                    SequenceState.LastKnownState.IN_FLIGHT))
        );
        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        Mockito.when(sequenceStateClient.getSequenceFromKafkaOffset(any(SourceTaskContext.class), anyString(), anyMap())).thenReturn(Optional.of(7L));
        mqSourceTask.start(createExactlyOnceConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        assertThat(mqSourceTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE);
        assertThat(mqSourceTask.getSequenceId().get()).isEqualTo(7L);
        assertThat(mqSourceTask.getMsgIds()).isEqualTo(messageIds);

        Mockito.verify(sequenceStateClient, Mockito.times(1)).replaceState(
                new SequenceState(
                        7,
                        messageIds,
                        SequenceState.LastKnownState.DELIVERED)
        );
    }

    @Test
    public void testPollsBlockUntilBatchComplete() throws JMSRuntimeException, JMSException, InterruptedException {
        Mockito.when(jmsWorker.receive(anyString(), any(QueueConfig.class), anyBoolean())).thenReturn(jmsMessage);

        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        mqSourceTask.start(createDefaultConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        List<SourceRecord> firstConnectMessagesBatch = mqSourceTask.poll();
        assertThat(firstConnectMessagesBatch.size()).isEqualTo(MQ_BATCH_SIZE);

        for (int i = 0; i < firstConnectMessagesBatch.size(); i++) {
            if (i < 2 || i > (MQ_BATCH_SIZE - 2)) {
                // do a few polls while messages are being committed, but
                //  keep under the limit that will cause an exception
                List<SourceRecord> pollDuringCommits = mqSourceTask.poll();
                assertThat(pollDuringCommits).isNull();
            }

            mqSourceTask.commitRecord(firstConnectMessagesBatch.get(i), null);
        }

        // now all messages are committed, a poll should return messages
        List<SourceRecord> secondConnectMessagesBatch = mqSourceTask.poll();
        assertThat(secondConnectMessagesBatch.size()).isEqualTo(MQ_BATCH_SIZE);
    }

    @Test
    public void testRepeatedPollsFailWhileMessagesInFlight() throws JMSRuntimeException, JMSException, InterruptedException {
        Mockito.when(jmsWorker.receive(anyString(), any(QueueConfig.class), anyBoolean())).thenReturn(jmsMessage);

        MQSourceTask mqSourceTask = new MQSourceTask();
        mqSourceTask.initialize(sourceTaskContext);
        mqSourceTask.start(createDefaultConnectorProperties(), jmsWorker, dedicatedWorker, sequenceStateClient);

        List<SourceRecord> firstConnectMessagesBatch = mqSourceTask.poll();
        assertThat(firstConnectMessagesBatch.size()).isEqualTo(MQ_BATCH_SIZE);

        final int BLOCKED_POLLS_LIMIT = 50;
        for (int i = 0; i < BLOCKED_POLLS_LIMIT; i++) {
            // up to the limit, polls before the batch is committed are
            //  allowed, but return null to indicate that the poll
            //  cycle was skipped
            List<SourceRecord> pollDuringCommits = mqSourceTask.poll();
            assertThat(pollDuringCommits).isNull();
        }

        // additional polls throw an exception to indicate that the
        //  task has been blocked for too long
        assertThrows(ConnectException.class, () -> mqSourceTask.poll());
    }
}
