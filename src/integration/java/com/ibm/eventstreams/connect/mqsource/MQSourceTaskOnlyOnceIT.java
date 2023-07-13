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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceState;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateClient;
import com.ibm.eventstreams.connect.mqsource.util.LogMessages;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithEmptyKafkaOffset;
import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithKafkaOffset;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.browseAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.removeAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getIDsOfMessagesCurrentlyOnQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.putAllMessagesToQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MessagesObjectMother.createAListOfMessages;
import static org.assertj.core.api.Assertions.assertThat;

public class MQSourceTaskOnlyOnceIT extends AbstractJMSContextIT {

    private MQSourceTask connectTask;
    private SequenceStateClient sequenceStateClient;

    @Before
    public void startup() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();
        removeAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        removeAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        final Map<String, String> connectorConfigProps = connectionProperties();

        final JMSWorker shared = new JMSWorker();
        shared.configure(connectorConfigProps);
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(connectorConfigProps);
        sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);

        connectTask.start(connectorConfigProps, shared, dedicated, sequenceStateClient);
    }

    @After
    public void cleanup() throws InterruptedException {
        final SourceTaskStopper stopper = new SourceTaskStopper(connectTask);
        stopper.run();

        sequenceStateClient.closeClientConnections();
    }

    private Map<String, String> connectionProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", QMGR_NAME);
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", DEFAULT_CONNECTION_NAME);
        props.put("mq.channel.name", CHANNEL_NAME);
        props.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("mq.message.body.jms", "true");
        props.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        props.put("mq.exactly.once.state.queue", DEFAULT_STATE_QUEUE);
        props.put("tasks.max", "1");
        return props;
    }

    private Map<String, String> connectionPropertiesWithOnlyOnceDisabled() {
        final Map<String, String> props = connectionProperties();
        props.put("mq.exactly.once.state.queue", null);
        return props;
    }

    @NotNull
    private List<Message> aListOfSomeMessages() throws Exception {
        return Arrays.asList(
                getJmsContext().createTextMessage("hello"),
                getJmsContext().createTextMessage("world")
        );
    }

    @Test
    public void testPollGetSequenceId_GivenSequenceStateIsPresentOnQueue() throws Exception {

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());
        List<String> allMessageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE);
        SequenceState deliveryState = new SequenceState(
                1,
                allMessageIds,
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(deliveryState);

        connectTask = getSourceTaskWithEmptyKafkaOffset();

        connectTask.start(connectionProperties());

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::value)
                .containsExactlyInAnyOrder("hello", "world");

        assertThat(connectTask.getSequenceId().get()).isEqualTo(1L);
    }

    @Test
    public void testPollGetsSequenceId_GivenSequenceStateIsNotPresentOnQueue() throws Exception {

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::value)
                .containsExactlyInAnyOrder("hello", "world");

        assertThat(connectTask.getSequenceId().get()).isEqualTo(1L);
    }

    @Test
    public void testPollEndsWithPutSequenceStateOnQueue_GivenMessagesHaveBeenReceivedFromQueue() throws Exception {

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());

        List<String> allMessageIDsOnQueue = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE, 2);

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(getSequenceStateAndAssertNotEmpty()).isEqualTo(new SequenceState(
                1L,
                allMessageIDsOnQueue,
                SequenceState.LastKnownState.IN_FLIGHT
        ));

        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::value)
                .containsExactlyInAnyOrder("hello", "world");
    }

    @Test
    public void testSequenceIDIncrementsBy1InLineWithBatchBehaviour_WhenPollIsCalled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = connectionProperties();
        connectorConfigProps.put("mq.batch.size", "10");

        connectTask.start(connectorConfigProps);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, createAListOfMessages(getJmsContext(), 35, "message "));

        pollAndAssert(10, 1L);

        pollAndAssert(10, 2L);

        pollAndAssert(10, 3L);

        pollAndAssert(5, 4L);
    }

    private void pollAndAssert(int expectedBatchSize, long sequenceId) throws Exception {

        List<String> messageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE, expectedBatchSize);

        List<SourceRecord> kafkaMessages = connectTask.poll();

        assertThat(kafkaMessages)
                .hasSize(expectedBatchSize)
                .extracting(SourceRecord::sourceOffset)
                .isNotNull()
                .extracting((sourceOffset) -> (Long) sourceOffset.get("sequence-id"))
                .contains(sequenceId);

        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m);
        }

        assertThat(getSequenceStateAndAssertNotEmpty()).isEqualTo(new SequenceState(
                sequenceId,
                messageIds,
                SequenceState.LastKnownState.IN_FLIGHT
        ));
    }

    private SequenceState getSequenceStateAndAssertNotEmpty() {
        SequenceState sequenceState;
        try {
            List<Message> stateMsgs = getAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
            assertThat(stateMsgs.size()).isEqualTo(1);
            ObjectMapper mapper = new ObjectMapper();
            sequenceState = mapper.readValue(stateMsgs.get(0).getBody(String.class), SequenceState.class);
        } catch (JMSException | IOException e) {
            throw new RuntimeException(e);
        };

        return sequenceState;
    }

    @Test
    public void testSourceOffset() throws Exception {
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());

        final List<SourceRecord> sourceRecords = connectTask.poll();

        for (final SourceRecord sourceRecord : sourceRecords) {
            connectTask.commitRecord(sourceRecord);
        }

        assertThat(sourceRecords)
                .hasSize(2)
                .extracting(SourceRecord::sourceOffset)
                .isNotNull()
                .extracting((sourceOffset) -> (Long) sourceOffset.get("sequence-id"))
                .containsExactlyInAnyOrder(1L, 1L);
    }

    @Test
    public void test_IfSequenceStateIsOnStateQueue_ThenActionIsREDELIVER_UNSENT_BATCH_SequenceIDIsValueFromStateQueue() throws Exception {
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());
        List<String> allMessageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE);
        SequenceState deliveryState = new SequenceState(
                2,
                allMessageIds,
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(deliveryState);

        MQSourceTask connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(connectionProperties());

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::value)
                .containsExactlyInAnyOrder("hello", "world");

        assertThat(connectTask.getSequenceId().get()).isEqualTo(2L);
    }

    @Test
    public void testConnectorFirstTimeRunStart_ActionIsStandardGet_AndStateNotInKafka() throws Exception {

        // i.e State is not in Kafka
        MQSourceTask connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(connectionProperties());

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::value)
                .containsExactlyInAnyOrder("hello", "world");

        assertThat(connectTask.getSequenceId().get()).isEqualTo(1L);
    }

    @Test
    public void testOnlyOnceDisabled_NoStateSaved_AndSequenceIdIncrements() throws Exception {

        // i.e State is not in Kafka
        MQSourceTask connectTask = getSourceTaskWithKafkaOffset(10L);
        connectTask.start(connectionPropertiesWithOnlyOnceDisabled());

        // Assert that nothing put here after SourceTask start
        assertThat(browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE)).isEmpty();

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());


        List<SourceRecord> kafkaMessages;

        // --- POLL 1

        kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::sourceOffset)
                .containsOnlyNulls();

        assertThat(connectTask.getSequenceId().get()).isEqualTo(0L);

        assertThat(browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE)).isEmpty();


        // --- POLL 2

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());

        kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::sourceOffset)
                .containsOnlyNulls();

        assertThat(connectTask.getSequenceId().get()).isEqualTo(0L);

        assertThat(browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE)).isEmpty();
    }

    @Test
    public void testPoll_WhenMessagesAreNotRolledBack_TimeOutConnectExceptionIsThrown() throws Exception {
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfSomeMessages());
        List<String> allMessageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE);
        List<String> oldMessageIds = new ArrayList<String>();
        oldMessageIds.add("0");
        oldMessageIds.add("1");
        assertThat(allMessageIds).doesNotContain("0");
        assertThat(allMessageIds).doesNotContain("1");
        SequenceState deliveryState = new SequenceState(
                2,
                oldMessageIds,
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(deliveryState);

        MQSourceTask connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(connectionProperties());
        for(int i = 0 ; i < 300 ; i ++) {
            connectTask.poll();
        }

        Exception exc = null;
        try{
            connectTask.poll();
        } catch (Exception e) {
            exc = e;
        }
        assertThat(exc).isNotNull();
        assertThat(exc).isInstanceOf(ConnectException.class);
        assertThat(exc.getMessage()).isEqualTo(LogMessages.rollbackTimeout(oldMessageIds));
    }
}