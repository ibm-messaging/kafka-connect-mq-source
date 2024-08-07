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

import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithEmptyKafkaOffset;
import static com.ibm.eventstreams.connect.mqsource.utils.MQQueueManagerAttrs.startChannel;
import static com.ibm.eventstreams.connect.mqsource.utils.MQQueueManagerAttrs.stopChannel;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.browseAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.removeAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getMessageCount;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.putAllMessagesToQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MessagesObjectMother.createAListOfMessages;
import static com.ibm.eventstreams.connect.mqsource.utils.MessagesObjectMother.listOfMessagesButOneIsMalformed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateClient;
import com.ibm.eventstreams.connect.mqsource.util.QueueConfig;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;

public class MQSourceTaskExceptionHandlingIT extends AbstractJMSContextIT {

    private static final Logger log = LoggerFactory.getLogger(MQTestUtil.class);

    private MQSourceTask connectTask;
    private SequenceStateClient sequenceStateClient;

    private static final String TOPIC_NAME = "mytopic";

    private SequenceStateClient createSequenceStateClient(final JMSWorker shared, final JMSWorker dedicated, final Map<String,String> connectorProps) {
        sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);
        return sequenceStateClient;
    }

    @Before
    public void startup() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();
        final Map<String, String> connectorConfigProps = getConnectorProps();

        JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(connectorConfigProps));

        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));

        connectTask.start(connectorConfigProps);
        createSequenceStateClient(shared, dedicated, connectorConfigProps);
    }

    @After
    public void cleanup() throws Exception {
        removeAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        removeAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        sequenceStateClient.closeClientConnections();

        final SourceTaskStopper stopper = new SourceTaskStopper(connectTask);
        stopper.run();
    }

    private Map<String, String> getConnectorProps() {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("mq.queue.manager", QMGR_NAME);
        connectorProps.put("mq.connection.mode", "client");
        connectorProps.put("mq.connection.name.list", "localhost(" + mqContainer.getMappedPort(1414).toString() + ")");
        connectorProps.put("mq.channel.name", CHANNEL_NAME);
        connectorProps.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        connectorProps.put("mq.user.authentication.mqcsp", "false");
        connectorProps.put("mq.message.body.jms", "true");
        connectorProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorProps.put("topic", TOPIC_NAME);
        return connectorProps;
    }

    @Test
    public void testPollDoesThrowExceptionDueToMQConnectionError() throws Exception {

        assertThat(getAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE).size()).isEqualTo(0);

        MQSourceTask connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = getConnectorProps();
        connectorConfigProps.put("mq.batch.size", "10");

        connectTask.start(connectorConfigProps);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 30, "stop queue message: ");

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE).size()).isEqualTo(30);

        stopChannel(QMGR_NAME, REST_API_HOST_PORT, ADMIN_PASSWORD);

        // Do a poll - this will fail as we've stopped the queue but throw no exception because it's being hidden.
        assertThrows(Exception.class, () -> connectTask.poll());

        startChannel(QMGR_NAME, REST_API_HOST_PORT, ADMIN_PASSWORD);

        assertThat(getAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE))
                .extracting((Message message) -> ((TextMessage) message).getText())
                .containsExactly(
                        "stop queue message: 1",
                        "stop queue message: 2",
                        "stop queue message: 3",
                        "stop queue message: 4",
                        "stop queue message: 5",
                        "stop queue message: 6",
                        "stop queue message: 7",
                        "stop queue message: 8",
                        "stop queue message: 9",
                        "stop queue message: 10",
                        "stop queue message: 11",
                        "stop queue message: 12",
                        "stop queue message: 13",
                        "stop queue message: 14",
                        "stop queue message: 15",
                        "stop queue message: 16",
                        "stop queue message: 17",
                        "stop queue message: 18",
                        "stop queue message: 19",
                        "stop queue message: 20",
                        "stop queue message: 21",
                        "stop queue message: 22",
                        "stop queue message: 23",
                        "stop queue message: 24",
                        "stop queue message: 25",
                        "stop queue message: 26",
                        "stop queue message: 27",
                        "stop queue message: 28",
                        "stop queue message: 29",
                        "stop queue message: 30"

                );
    }

    @Test
    public void testIfErrorsOccurWithPollRollbackAndContinues() throws Exception {
        JMSWorker spyJMSWorker = Mockito.spy(new JMSWorker());
        spyJMSWorker.configure(getPropertiesConfig(getConnectorProps()));

        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(getConnectorProps()));

        MQSourceTask connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = getConnectorProps();
        connectorConfigProps.put("mq.batch.size", "5");

        connectTask.start(connectorConfigProps, spyJMSWorker, dedicated, createSequenceStateClient(spyJMSWorker, dedicated, connectorConfigProps));

        final List<Message> messages = createAListOfMessages(getJmsContext(), 15, "stop midway through poll queue message: ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        doCallRealMethod()
                .doCallRealMethod()
                .doCallRealMethod()
                .doCallRealMethod()
                .doCallRealMethod() // 5
                .doCallRealMethod()
                .doCallRealMethod()
                .doAnswer((Void) -> {
                    stopChannel(QMGR_NAME, REST_API_HOST_PORT, ADMIN_PASSWORD);
                    return getJmsContext().createTextMessage("Spy Injected Message");
                })
                .doCallRealMethod() // --> failure
                .when(spyJMSWorker).receive(anyString(),any(QueueConfig.class), anyBoolean());

        pollCommitAndAssert(connectTask, 5, 10);

        // Do a poll - this will fail as we've stopped the queue and throws a retriable exception.
        List<SourceRecord> sourceRecords = Collections.EMPTY_LIST;
        Exception exc = null;
        try{
            sourceRecords = connectTask.poll();
        } catch (Exception e) {
            exc = e;
        }
        assertThat(exc).isNotNull();
        assertThat(exc).isInstanceOf(RetriableException.class);
        for (SourceRecord record : sourceRecords) {
            connectTask.commitRecord(record);
        }

        assertThat(sourceRecords.size()).isEqualTo(0);

        // fix our queue so we can continue...
        startChannel(QMGR_NAME, REST_API_HOST_PORT, ADMIN_PASSWORD);

        assertThat(getMessageCount(DEFAULT_SOURCE_QUEUE)).isEqualTo(10);

        pollCommitAndAssert(connectTask, 5, 5);

        pollCommitAndAssert(connectTask, 5, 0);

        pollCommitAndAssert(connectTask, 0, 0);

        assertThat(getMessageCount(DEFAULT_SOURCE_QUEUE)).isEqualTo(0);
    }

    @Test
    public void testNoExceptionThrownWhenJMSExceptionThrownByBuilder_AndRecordsAreRollbackAndThenProcessedNextPoll() throws Exception {
        JMSWorker sharedJMSWorker = new JMSWorker();
        sharedJMSWorker.configure(getPropertiesConfig(getConnectorProps()));

        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(getConnectorProps()));

        DefaultRecordBuilder spyRecordBuilder = Mockito.spy(new DefaultRecordBuilder());

        MQSourceTask connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = getConnectorProps();
        connectorConfigProps.put("mq.batch.size", "5");

        connectTask.start(
                connectorConfigProps,
                sharedJMSWorker,
                dedicated,
                createSequenceStateClient(sharedJMSWorker, dedicated, connectorConfigProps)
        );

        final List<Message> messages = createAListOfMessages(getJmsContext(), 15, "Builder exception message: ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        assertThat(getMessageCount(DEFAULT_SOURCE_QUEUE)).isEqualTo(15);

        doCallRealMethod()
                .doCallRealMethod()
                .doCallRealMethod()
                .doCallRealMethod()
                .doCallRealMethod() // 5
                .doCallRealMethod()
                .doCallRealMethod()
                .doThrow(new JMSException("This is a JMSException caused by a spy!!")) // 8
                .doCallRealMethod()
                 // Be careful with these, any() does not cover null hence null is used
                .when(spyRecordBuilder).toSourceRecord(any(JMSContext.class), eq(TOPIC_NAME), anyBoolean(), any(Message.class), any(), any());

        // Needs to be done here, after the doCallRealMethods have been setup.
        sharedJMSWorker.setRecordBuilder(spyRecordBuilder);

        // Poll no issues, round 1
        pollCommitAndAssert(connectTask, 5, 10);

        // Do a poll - this will fail as we've stopped the queue and an exception is thrown.
        pollCommitAndAssert(connectTask, 0, 10);

        // If batch complete signal is not null, program will hang forever.
        assertThat(connectTask.getBatchCompleteSignal()).isNull();

        pollCommitAndAssert(connectTask, 5, 5);

        pollCommitAndAssert(connectTask, 5, 0);

        pollCommitAndAssert(connectTask, 0, 0);
    }

    @Test
    public void verifyMessageBatchRollback() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = getConnectorProps();
        connectorConfigProps.put("mq.batch.size", "10");

        connectTask.start(connectorConfigProps);

        final List<Message> messages = listOfMessagesButOneIsMalformed(getJmsContext());

        Thread.sleep(5000L);
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        final List<SourceRecord> kafkaMessages;

        // first batch should successfully retrieve messages 01-10
        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        connectTask.commit();
        connectTask.commit();

        // second batch (11-20) should fail because of message 16
        final ConnectException exc = assertThrows(ConnectException.class, () -> {
            connectTask.poll();
        });

        assertEquals("com.ibm.eventstreams.connect.mqsource.builders.RecordBuilderException: Unsupported JMS message type", exc.getMessage());

        // there should be 20 messages left on the MQ queue (messages 11-30)
        connectTask.stop();
        final List<Message> remainingMQMessages = getAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertEquals(20, remainingMQMessages.size());
    }


    private static void pollCommitAndAssert(MQSourceTask connectTask, int recordsProcessed, int recordsLeft) throws Exception {

        List<SourceRecord> sourceRecords = new ArrayList<>();

        try {
            sourceRecords = connectTask.poll();
            for (SourceRecord record : sourceRecords) {
                connectTask.commitRecord(record);
            }

        } catch(Exception e) {
            log.info("exception caught and thrown away during test: " + e);
        }

        assertThat(sourceRecords.size()).isEqualTo(recordsProcessed);

        assertThat(getMessageCount(DEFAULT_SOURCE_QUEUE)).isEqualTo(recordsLeft);
    }

}
