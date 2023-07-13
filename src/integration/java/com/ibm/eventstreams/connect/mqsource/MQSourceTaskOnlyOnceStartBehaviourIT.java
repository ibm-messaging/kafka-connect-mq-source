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

import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceState;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateClient;
import com.ibm.eventstreams.connect.mqsource.utils.MQConnectionRollbackHelper;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.Message;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithContext;
import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithEmptyKafkaOffset;
import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithKafkaOffset;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.browseAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.removeAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getIDsOfMessagesCurrentlyOnQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.putAllMessagesToQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.SourceTaskContextObjectMother.SourceTaskContextWithOffsetId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

public class MQSourceTaskOnlyOnceStartBehaviourIT extends AbstractJMSContextIT {

    private MQSourceTask connectTask;
    private SequenceStateClient sequenceStateClient;

    @Before
    public void startup() throws Exception {

        JMSWorker shared = new JMSWorker();
        shared.configure(connectionProperties());
        shared.connect();
        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(connectionProperties());
        dedicated.connect();
        sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);

        removeAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        removeAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
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

    @NotNull
    private List<Message> aListOfTwoMessages() throws Exception {
        return Arrays.asList(
                getJmsContext().createTextMessage("hello"),
                getJmsContext().createTextMessage("world")
        );
    }

    @NotNull
    private List<Message> aListOfEightMessages() throws Exception {
        return Arrays.asList(
                getJmsContext().createTextMessage("hello"),
                getJmsContext().createTextMessage("world"),
                getJmsContext().createTextMessage("this"),
                getJmsContext().createTextMessage("is"),
                getJmsContext().createTextMessage("a"),
                getJmsContext().createTextMessage("longer"),
                getJmsContext().createTextMessage("test"),
                getJmsContext().createTextMessage("!")
        );
    }

    @Test
    public void testOnlyOnceStartBehaviour_GivenNoSequenceStateIsPresentOnQueueOrKafka() throws Exception {

        connectTask = getSourceTaskWithEmptyKafkaOffset(); // Kafka has no state

        connectTask.start(connectionProperties());

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfTwoMessages());
        List<String> msgIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE);

        List<Message> messages = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(messages).isEmpty(); // Check that MQ has no state

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        // Check that MQ has been read from
        List<Message> mqMessages = browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(mqMessages).isEmpty(); 
        
        // Check that the messages have been created as source records correctly
        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::sourceOffset)
                .isNotNull()
                .extracting((sourceOffset) -> (Long) sourceOffset.get("sequence-id"))
                .containsExactlyInAnyOrder(1L, 1L);

        // Check that the internal sequence id has been set correctly
        assertThat(connectTask.getSequenceId().get()).isEqualTo(1L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceState = sequenceStateClient.browse();
        assertThat(mqSequenceState)
                .isNotEmpty()
                .get()
                .isEqualTo(new SequenceState(1L, msgIds, SequenceState.LastKnownState.IN_FLIGHT));
    }

    @Test
    public void testOnlyOnceStartBehaviour_GivenNoSequenceStateIsPresentOnQueueButOffsetIsPresentInKafka() throws Exception {

        connectTask = getSourceTaskWithKafkaOffset(); // Kafka has state

        connectTask.start(connectionProperties());

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfTwoMessages());

        List<Message> messages = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(messages).isEmpty(); // Check that MQ has no state

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessages) {
            connectTask.commitRecord(kafkaMessage);
        }

        // Check that MQ has been read from
        List<Message> mqMessages = browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(mqMessages).isEmpty(); 
        
        // Check that the messages have been created as source records correctly
        assertThat(kafkaMessages)
                .hasSize(2)
                .extracting(SourceRecord::sourceOffset)
                .isNotNull()
                .extracting((sourceOffset) -> (Long) sourceOffset.get("sequence-id"))
                .containsExactlyInAnyOrder(6L, 6L);

        // Check that the internal sequence id has been set correctly
        assertThat(connectTask.getSequenceId().get()).isEqualTo(6L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceState = sequenceStateClient.browse();
        assertThat(mqSequenceState).isNotEmpty();
        assertThat(mqSequenceState.get().getSequenceId()).isEqualTo(6L);
    }

    @Test // this one needs to take in to account the message ids that should be re deilvered from MQ Also need assertion on state defined inthe start command 
    public void testOnlyOnceStartBehaviour_GivenSequenceStateIsPresentOnQueue_AndStateIsInFlight_AndStoredOffsetNotInKafka() throws Exception {

        connectTask = getSourceTaskWithEmptyKafkaOffset(); // Kafka has no state

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfEightMessages()); //adding 8 messages to source q

        List<String> allMessageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE); // getting all message ids for use in setting up state q entry and assertions
        List<String> firstBatchOfMessageIds = allMessageIds.subList(0, 5);
        SequenceState deliveryState = new SequenceState(
            23,
            firstBatchOfMessageIds,
            SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(deliveryState);
        MQConnectionRollbackHelper rollbackTestHelper = new MQConnectionRollbackHelper();
        rollbackTestHelper.readNoCommit(DEFAULT_SOURCE_QUEUE, 5);

        Map<String, String> props = connectionProperties();
        props.put("mq.batch.size", "5");

        connectTask.start(props); // this is a start after a crash

        assertThat(connectTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REDELIVER_UNSENT_BATCH);
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);
        assertThat(connectTask.getMsgIds()).isEqualTo(firstBatchOfMessageIds);
        
        final List<SourceRecord> kafkaMessagesInitialPoll = connectTask.poll();

        // Check that no record have been returned since the inflight records transaction has ent been rolled back yet
        assertThat(kafkaMessagesInitialPoll.size()).isEqualTo(0);

        // Check that the internal sequence id has not been incremented by poll since this poll call should return an empty list
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceState = sequenceStateClient.browse();
        assertThat(mqSequenceState).isNotEmpty();
        assertThat(mqSequenceState.get().getSequenceId()).isEqualTo(23L);

        rollbackTestHelper.rollback();

        final List<SourceRecord> kafkaMessagesSecondPoll = connectTask.poll();

        // Check that the messages have been created as source records correctly
        assertThat(kafkaMessagesSecondPoll)
                .hasSize(5)
                .extracting(SourceRecord::sourceOffset)
                .isNotNull()
                .extracting((sourceOffset) -> (Long) sourceOffset.get("sequence-id"))
                .containsExactlyInAnyOrder(23L, 23L, 23L, 23L, 23L);

        // Check that MQ has been read from
        List<Message> mqMessages = browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(mqMessages.size()).isEqualTo(3); 

        // Check that the internal sequence id has been set correctly
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceStateSecondPoll = sequenceStateClient.browse();
        assertThat(mqSequenceStateSecondPoll).isNotEmpty();
        assertThat(mqSequenceStateSecondPoll.get().getSequenceId()).isEqualTo(23L);
    }

    @Test
    public void testOnlyOnceStartBehaviour_GivenSequenceStateIsPresentOnQueue_AndStateIsInFlight_AndStoredOffsetInKafka_AndMqSequenceDoesNotMatch() throws Exception {
        connectTask = getSourceTaskWithKafkaOffset(22L);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfEightMessages()); //adding 8 messages to source q

        List<String> allMessageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE); // getting all message ids for use in setting up state q entry and assertions
        List<String> firstBatchOfMessageIds = allMessageIds.subList(0, 5);

        SequenceState deliveryState = new SequenceState(
                23,
                firstBatchOfMessageIds,
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(deliveryState);
        MQConnectionRollbackHelper rollbackTestHelper = new MQConnectionRollbackHelper();
        rollbackTestHelper.readNoCommit(DEFAULT_SOURCE_QUEUE, 5);

        Map<String, String> props = connectionProperties();
        props.put("mq.batch.size", "5");

        connectTask.start(props); // this is a start after a crash

        assertThat(connectTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REDELIVER_UNSENT_BATCH);
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);
        assertThat(connectTask.getMsgIds()).isEqualTo(firstBatchOfMessageIds);

        final List<SourceRecord> kafkaMessagesInitialPoll = connectTask.poll();

        // Check that no record have been returned since the inflight records transaction has ent been rolled back yet
        assertThat(kafkaMessagesInitialPoll.size()).isEqualTo(0);

        // Check that the internal sequence id has not been incremented by poll since this poll call should return an empty list
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceState = sequenceStateClient.browse();
        assertThat(mqSequenceState).isNotEmpty();
        assertThat(mqSequenceState.get().getSequenceId()).isEqualTo(23L);

        rollbackTestHelper.rollback();

        final List<SourceRecord> kafkaMessagesSecondPoll = connectTask.poll();

        // Check that the messages have been created as source records correctly
        assertThat(kafkaMessagesSecondPoll)
                .hasSize(5)
                .extracting(SourceRecord::sourceOffset)
                .isNotNull()
                .extracting((sourceOffset) -> (Long) sourceOffset.get("sequence-id"))
                .containsExactlyInAnyOrder(23L, 23L, 23L, 23L, 23L);

        // Check that MQ has been read from
        List<Message> mqMessages = browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(mqMessages.size()).isEqualTo(3);

        // Check that the internal sequence id has been set correctly
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceStateSecondPoll = sequenceStateClient.browse();
        assertThat(mqSequenceStateSecondPoll).isNotEmpty();
        assertThat(mqSequenceStateSecondPoll.get().getSequenceId()).isEqualTo(23L);
    }




    @Test
    public void testOnlyOnceStartBehaviour_GivenSequenceStateIsPresentOnQueue_AndStateIsInFlight_AndStoredOffsetInKafka_AndMqSequenceDoMatch() throws Exception {
        connectTask = getSourceTaskWithKafkaOffset(23L);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfEightMessages()); //adding 8 messages to source q

        List<String> allMessageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE); // getting all message ids for use in setting up state q entry and assertions
        List<String> firstBatchOfMessageIds = allMessageIds.subList(0, 5);

        SequenceState deliveryState = new SequenceState(
                23,
                firstBatchOfMessageIds,
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(deliveryState);

        // The next two lines hold the first 5 messages on the source queue in a tx so poll won't find them yet. rollbackTestHelper will roll back later to make them available
        MQConnectionRollbackHelper rollbackTestHelper = new MQConnectionRollbackHelper();
        rollbackTestHelper.readNoCommit(DEFAULT_SOURCE_QUEUE, 5);

        Map<String, String> props = connectionProperties();
        props.put("mq.batch.size", "5");

        connectTask.start(props); // this is a start after a crash

        assertThat(connectTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE);
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);
        assertThat(connectTask.getMsgIds()).isEqualTo(firstBatchOfMessageIds);

        Optional<SequenceState> mqSequenceStateInitialAfterStart = sequenceStateClient.browse();

        assertThat(mqSequenceStateInitialAfterStart).isNotEmpty();
        assertThat(mqSequenceStateInitialAfterStart.get().getSequenceId()).isEqualTo(23L);
        assertThat(mqSequenceStateInitialAfterStart.get().getMessageIds()).isEqualTo(firstBatchOfMessageIds);
        assertThat(mqSequenceStateInitialAfterStart.get().getLastKnownState()).isEqualTo(SequenceState.LastKnownState.DELIVERED);

        final List<SourceRecord> kafkaMessagesInitialPoll = connectTask.poll();

        // Check that no record have been returned since the inflight records transaction has ent been rolled back yet
        assertThat(kafkaMessagesInitialPoll.size()).isEqualTo(0);

        // Check that the internal sequence id has not been incremented by poll since this poll call should return an empty list
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceState = sequenceStateClient.browse();
        assertThat(mqSequenceState).isNotEmpty();
        assertThat(mqSequenceState.get().getSequenceId()).isEqualTo(23L);

        rollbackTestHelper.rollback();

        final List<SourceRecord> kafkaMessagesSecondPoll = connectTask.poll();

        // Check that the messages have not been passed to Kafka again
        assertThat(kafkaMessagesSecondPoll).hasSize(0);

        // Check that we're now back to normal operation
        assertThat(connectTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.NORMAL_OPERATION);

        // Check that MQ has been read from
        List<Message> mqMessages = browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(mqMessages.size()).isEqualTo(3); // only the DELIVERED batch should have been removed

        // Check that MQ sequence state has been resolved/cleared correctly
        Optional<SequenceState> mqSequenceStateSecondPoll = sequenceStateClient.browse();
        assertThat(mqSequenceStateSecondPoll).isEmpty();

        final List<SourceRecord> kafkaMessagesThirdPoll = connectTask.poll();
        Optional<SequenceState> mqSequenceStateThirdPoll = sequenceStateClient.browse();

        assertThat(kafkaMessagesThirdPoll).hasSize(3);
        SequenceState sequenceStateThirdPoll  = mqSequenceStateThirdPoll.get();
        assertThat(sequenceStateThirdPoll.getSequenceId()).isEqualTo(24L); //new state with increased sequence-id should be there
        assertThat(sequenceStateThirdPoll.getMessageIds().size()).isEqualTo(3); //there should be three msgIds;
        assertThat(sequenceStateThirdPoll.isInFlight()).isTrue(); //sequence should be in-flight

    }

    @Test
    public void testOnlyOnceStartBehaviour_GivenSequenceStateIsPresentOnQueue_AndStateIsIsDelivered() throws Exception {
        connectTask = getSourceTaskWithKafkaOffset(22L);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, aListOfEightMessages()); //adding 8 messages to source q

        List<String> allMessageIds = getIDsOfMessagesCurrentlyOnQueue(DEFAULT_SOURCE_QUEUE); // getting all message ids for use in setting up state q entry and assertions
        List<String> firstBatchOfMessageIds = allMessageIds.subList(0, 5);

        SequenceState deliveryState = new SequenceState(
                23,
                firstBatchOfMessageIds,
                SequenceState.LastKnownState.DELIVERED
        );

        sequenceStateClient.write(deliveryState);
        MQConnectionRollbackHelper rollbackTestHelper = new MQConnectionRollbackHelper();
        rollbackTestHelper.readNoCommit(DEFAULT_SOURCE_QUEUE, 5);

        Map<String, String> props = connectionProperties();
        props.put("mq.batch.size", "5");

        connectTask.start(props); // this is a start after a crash

        assertThat(connectTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE);
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);
        assertThat(connectTask.getMsgIds()).isEqualTo(firstBatchOfMessageIds);

        final List<SourceRecord> kafkaMessagesInitialPoll = connectTask.poll();

        assertThat(kafkaMessagesInitialPoll.size()).isEqualTo(0);

        // Check that the internal sequence id has not been incremented by poll since this poll call should return an empty list
        assertThat(connectTask.getSequenceId().get()).isEqualTo(23L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceState = sequenceStateClient.browse();
        assertThat(mqSequenceState).isNotEmpty();
        assertThat(mqSequenceState.get().getSequenceId()).isEqualTo(23L);

        rollbackTestHelper.rollback();

        final List<SourceRecord> kafkaMessagesSecondPoll = connectTask.poll();
        assertThat(kafkaMessagesSecondPoll).hasSize(0);
        assertThat(connectTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.NORMAL_OPERATION);

        final List<SourceRecord> kafkaMessagesThirdPoll = connectTask.poll();
        // Check that the messages have been created as source records correctly
        assertThat(kafkaMessagesThirdPoll)
                .hasSize(3)
                .extracting(SourceRecord::sourceOffset)
                .isNotNull()
                .extracting((sourceOffset) -> (Long) sourceOffset.get("sequence-id"))
                .containsOnly(24L);

        // Check that MQ has been read from
        List<Message> mqMessages = browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(mqMessages.size()).isEqualTo(0);

        // Check that the internal sequence id has been set correctly
        assertThat(connectTask.getSequenceId().get()).isEqualTo(24L);

        // Check that MQ sequence state has been set correctly
        Optional<SequenceState> mqSequenceStateSecondPoll = sequenceStateClient.browse();
        assertThat(mqSequenceStateSecondPoll).isNotEmpty();
        assertThat(mqSequenceStateSecondPoll.get().getSequenceId()).isEqualTo(24L);
    }

    @Test
    public void testOnlyOnceStartBehaviour_CrashAfterKafkaCommitBeforeMQCommit() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = connectionProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.batch.size", "2");

        connectTask.start(connectorConfigProps);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE,
                Arrays.asList(
                        getJmsContext().createTextMessage("hello"),
                        getJmsContext().createTextMessage("world"),
                        getJmsContext().createTextMessage("more"),
                        getJmsContext().createTextMessage("messages")
                )
        );

        final List<SourceRecord> kafkaMessagesRoundOne = connectTask.poll();

        assertThat(kafkaMessagesRoundOne.size()).isEqualTo(2);

        for (final SourceRecord kafkaMessage : kafkaMessagesRoundOne) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessagesRoundOne)
                .extracting(ConnectRecord::value)
                .containsExactly(
                        "hello", "world"
                );

        assertThat(browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE).size()).isEqualTo(2);

        //// --- POLL 2

        final List<SourceRecord> kafkaMessagesRoundTwo = connectTask.poll();

        assertThat(kafkaMessagesRoundTwo.size()).isEqualTo(2);

        for (final SourceRecord kafkaMessage : kafkaMessagesRoundTwo) {
            connectTask.commitRecord(kafkaMessage);
        }

        assertThat(kafkaMessagesRoundTwo)
                .extracting(ConnectRecord::value)
                .containsExactly(
                        "more", "messages"
                );

        assertThat(browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE).size()).isEqualTo(0);

        long offsetId = connectTask.getSequenceId().get();

        // To simulate crash + restart  ---> Happening before mq commit has occurred
        connectTask.stop();
        MQSourceTask connectTask = getSourceTaskWithContext(SourceTaskContextWithOffsetId(offsetId));
        connectTask.start(connectorConfigProps);

        /// ---- POLL 3
        final List<SourceRecord> kafkaMessagesRoundThree = connectTask.poll();

        for (final SourceRecord kafkaMessage : kafkaMessagesRoundThree) {
            connectTask.commitRecord(kafkaMessage);
        }

        // These messages would have been returned if onlyOnce wasn't implemented.
        assertThat(kafkaMessagesRoundThree)
                .extracting(ConnectRecord::value)
                .doesNotContain(
                        "more", "messages"
                );

        assertThat(kafkaMessagesRoundThree).isEmpty();

        assertThat(browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE).size()).isEqualTo(0);
    }

    @Test
    public void testOnceOnceStart_ThrowErrorWhenThereAreMultipleStateMsgs() throws JMSException {

        connectTask = getSourceTaskWithEmptyKafkaOffset();
        final Map<String, String> connectorConfigProps = connectionProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.batch.size", "2");

        final SequenceState sampleState = new SequenceState(1, new ArrayList<>(Arrays.asList("414d51204d59514d475220202020202033056b6401d50010")),SequenceState.LastKnownState.IN_FLIGHT);
        sequenceStateClient.write(sampleState);
        sequenceStateClient.write(sampleState);

        assertThrows(Exception.class, ()-> connectTask.start(connectorConfigProps));
    }
}