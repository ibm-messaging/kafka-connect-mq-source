/**
 * Copyright 2022, 2023, 2024 IBM Corporation
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
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.browseAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.putAllMessagesToQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MessagesObjectMother.createAListOfMessages;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateClient;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateException;
import com.ibm.eventstreams.connect.mqsource.util.QueueConfig;
import com.ibm.eventstreams.connect.mqsource.utils.JsonRestApi;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;



public class MQSourceTaskIT extends AbstractJMSContextIT {

    private MQSourceTask connectTask = null;

    @After
    public void after() throws InterruptedException {
        final SourceTaskStopper stopper = new SourceTaskStopper(connectTask);
        stopper.run();
    }

    @Before
    public void before() throws JMSException {
        MQTestUtil.removeAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        MQTestUtil.removeAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
    }

    private Map<String, String> createDefaultConnectorProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", QMGR_NAME);
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", DEFAULT_CONNECTION_NAME);
        props.put("mq.channel.name", CHANNEL_NAME);
        props.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("topic", "mytopic");
        props.put("mq.receive.timeout", "5000");
        props.put("mq.reconnect.delay.min.ms", "100");
        props.put("mq.reconnect.delay.max.ms", "10000");
        return props;
    }

    private Map<String, String> createExactlyOnceConnectorProperties() {
        final Map<String, String> props = createDefaultConnectorProperties();
        props.put("mq.exactly.once.state.queue", DEFAULT_STATE_QUEUE);
        props.put("tasks.max", "1");
        return props;
    }

    @Test
    public void verifyJmsTextMessages() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);
        assertThat(connectTask.startUpAction).isEqualTo(MQSourceTaskStartUpAction.NORMAL_OPERATION);
        final TextMessage message1 = getJmsContext().createTextMessage("hello");
        final TextMessage message2 = getJmsContext().createTextMessage("world");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message1, message2));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(2, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertNull(kafkaMessage.key());
            assertNull(kafkaMessage.valueSchema());

            connectTask.commitRecord(kafkaMessage);
        }

        assertEquals("hello", kafkaMessages.get(0).value());
        assertEquals("world", kafkaMessages.get(1).value());
    }

    @Test
    public void verifyJmsJsonMessages() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        final List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messages.add(getJmsContext().createTextMessage(
                    "{ " +
                            "\"i\" : " + i +
                            "}"));
        }
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(5, kafkaMessages.size());
        for (int i = 0; i < 5; i++) {
            final SourceRecord kafkaMessage = kafkaMessages.get(i);
            assertNull(kafkaMessage.key());
            assertNull(kafkaMessage.valueSchema());

            final Map<?, ?> value = (Map<?, ?>) kafkaMessage.value();
            assertEquals(Long.valueOf(i), value.get("i"));

            connectTask.commitRecord(kafkaMessage);
        }
    }

    @Test
    public void verifyMQMessage() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "false"); //this could also be absent but if set to true the test should fail
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final String sent = "Hello World";
        final String url = "https://localhost:" + REST_API_HOST_PORT + "/ibmmq/rest/v1/messaging/qmgr/" + QMGR_NAME + "/queue/DEV.QUEUE.1/message";
        JsonRestApi.postString(url, "app", ADMIN_PASSWORD, sent);

        final List<SourceRecord> kafkaMessages = connectTask.poll(); // get all the SRs (1)
        SourceRecord firstMsg = kafkaMessages.get(0);
        Object received = firstMsg.value();

        assertNotEquals(received.getClass(), String.class); //jms messages are retrieved as Strings
        assertEquals(received.getClass(), byte[].class);
        assertEquals(new String((byte[]) received, StandardCharsets.UTF_8), sent);

        connectTask.commitRecord(firstMsg);
        connectTask.poll();
    }

    @Test
    public void verifyJmsMessageHeaders() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.jms.properties.copy.to.kafka.headers", "true");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("helloworld");
        message.setStringProperty("teststring", "myvalue");
        message.setIntProperty("volume", 11);
        message.setDoubleProperty("decimalmeaning", 42.0);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());
        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertNull(kafkaMessage.key());
        assertNull(kafkaMessage.valueSchema());

        assertEquals("helloworld", kafkaMessage.value());

        assertEquals("myvalue", kafkaMessage.headers().lastWithName("teststring").value());
        assertEquals("11", kafkaMessage.headers().lastWithName("volume").value());
        assertEquals("42.0", kafkaMessage.headers().lastWithName("decimalmeaning").value());

        connectTask.commitRecord(kafkaMessage);
    }

    @Test
    public void verifyMessageBatchIndividualCommits() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();

        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.batch.size", "10");

        connectTask.start(connectorConfigProps);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 35, "batch message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        int nextExpectedMessage = 1;

        List<SourceRecord> kafkaMessages;

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertEquals("batch message " + (nextExpectedMessage++), kafkaMessage.value());
            connectTask.commitRecord(kafkaMessage);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertEquals("batch message " + (nextExpectedMessage++), kafkaMessage.value());
            connectTask.commitRecord(kafkaMessage);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertEquals("batch message " + (nextExpectedMessage++), kafkaMessage.value());
            connectTask.commitRecord(kafkaMessage);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(5, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertEquals("batch message " + (nextExpectedMessage++), kafkaMessage.value());
            connectTask.commitRecord(kafkaMessage);
        }
    }

    @Test
    public void verifyMessageBatchGroupCommits() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.batch.size", "10");

        connectTask.start(connectorConfigProps);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 35, "message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        List<SourceRecord> kafkaMessages;

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(5, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m);
        }
    }

    @Test
    public void verifyMessageIdAsKey() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.record.builder.key.header", "JMSMessageID");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("testmessage");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertEquals(message.getJMSMessageID().substring("ID:".length()), kafkaMessage.key());
        assertNotNull(message.getJMSMessageID());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, kafkaMessage.keySchema());

        assertEquals("testmessage", kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage);
    }

    @Test
    public void verifyCorrelationIdAsKey() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.record.builder.key.header", "JMSCorrelationID");

        connectTask.start(connectorConfigProps);

        final TextMessage message1 = getJmsContext().createTextMessage("first message");
        message1.setJMSCorrelationID("verifycorrel");
        final TextMessage message2 = getJmsContext().createTextMessage("second message");
        message2.setJMSCorrelationID("ID:5fb4a18030154fe4b09a1dfe8075bc101dfe8075bc104fe4");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message1, message2));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(2, kafkaMessages.size());

        final SourceRecord kafkaMessage1 = kafkaMessages.get(0);
        assertEquals("verifycorrel", kafkaMessage1.key());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, kafkaMessage1.keySchema());
        assertEquals("first message", kafkaMessage1.value());
        connectTask.commitRecord(kafkaMessage1);

        final SourceRecord kafkaMessage2 = kafkaMessages.get(1);
        assertEquals("5fb4a18030154fe4b09a1dfe8075bc101dfe8075bc104fe4", kafkaMessage2.key());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, kafkaMessage2.keySchema());
        assertEquals("second message", kafkaMessage2.value());
        connectTask.commitRecord(kafkaMessage2);
    }

    @Test
    public void verifyCorrelationIdBytesAsKey() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.record.builder.key.header", "JMSCorrelationIDAsBytes");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("testmessagewithcorrelbytes");
        message.setJMSCorrelationID("verifycorrelbytes");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertArrayEquals("verifycorrelbytes".getBytes(), (byte[]) kafkaMessage.key());
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, kafkaMessage.keySchema());

        assertEquals("testmessagewithcorrelbytes", kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage);
    }

    @Test
    public void verifyDestinationAsKey() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.record.builder.key.header", "JMSDestination");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("testmessagewithdest");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertEquals("queue:///" + DEFAULT_SOURCE_QUEUE, kafkaMessage.key());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, kafkaMessage.keySchema());

        assertEquals("testmessagewithdest", kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage);
    }

    @Test
    public void testSequenceStateMsgReadUnderMQTx() throws Exception {
        JMSWorker spyJMSWorker = Mockito.spy(new JMSWorker());

        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        spyJMSWorker.configure(getPropertiesConfig(connectorConfigProps));
        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));
        SequenceStateClient sequenceStateClient = Mockito.spy(new SequenceStateClient(DEFAULT_STATE_QUEUE, spyJMSWorker, dedicated));

        connectTask.start(connectorConfigProps, spyJMSWorker, dedicated, sequenceStateClient);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 2, "message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        List<SourceRecord> kafkaMessages;
        kafkaMessages = connectTask.poll();

        List<Message> stateMsgs1 = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs1.size()).isEqualTo(1);

        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m);
        }

        /// make commit do rollback when poll is called
        doAnswer((Void) -> {
            spyJMSWorker.getContext().rollback();
            throw new Exception("such an exception");

            }).when(spyJMSWorker).commit();

        try {
            connectTask.poll();
        } catch (Exception e) {
            System.out.println("exception caught");
        }


        /// expect statequeue to not be empty
        List<Message> stateMsgs2 = getAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs2.size()).isEqualTo(1);

        List<Message> sourceMsgs = getAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(sourceMsgs.size()).isEqualTo(2);


    }

    @Test
    public void testSequenceStateMsgWrittenIndependentFromGetSource() throws Exception {
        // setup test condition: put messages on source queue, poll once to read them
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(connectorConfigProps));
        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));
        SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);

        connectTask.start(connectorConfigProps, shared, dedicated, sequenceStateClient);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 2, "message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        connectTask.poll();

        List<Message> stateMsgs1 = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs1.size()).isEqualTo(1);
        shared.attemptRollback();
        assertThat(stateMsgs1.size()).isEqualTo(1); //state message is still there even though source message were rolled back

    }

    @Test
    public void testRemoveDeliveredMessagesFromSourceQueueThrowsException() throws Exception {

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        JMSWorker spyJMSWorker = Mockito.spy(new JMSWorker());
        JMSWorker spyDedicated = Mockito.spy(new JMSWorker());
        JMSWorker spyShared = Mockito.spy(new JMSWorker());

        spyJMSWorker.configure(getPropertiesConfig(connectorConfigProps));
        spyDedicated.configure(getPropertiesConfig(connectorConfigProps));
        spyShared.configure(getPropertiesConfig(connectorConfigProps));

        Message messageSpy = Mockito.spy(getJmsContext().createTextMessage("Spy Injected Message"));

        doReturn("6")
                .when(messageSpy)
                .getJMSMessageID();

        doReturn(messageSpy)
                .when(spyJMSWorker).receive(
                        anyString(),
                        any(QueueConfig.class),
                        anyBoolean());

        connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(connectorConfigProps, spyJMSWorker, spyDedicated, new SequenceStateClient(DEFAULT_STATE_QUEUE, spyShared, spyJMSWorker));

        String[] msgIds = new String[] {"1", "2"};

        assertThrows(SequenceStateException.class,
                () -> connectTask.removeDeliveredMessagesFromSourceQueue(Arrays.asList(msgIds))
        );
    }


    @Test
    public void testRemoveDeliveredMessagesFromSourceQueueDoesNotThrowException() throws Exception {

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        JMSWorker spyJMSWorker = Mockito.spy(new JMSWorker());
        JMSWorker spyDedicated = Mockito.spy(new JMSWorker());
        JMSWorker spyShared = Mockito.spy(new JMSWorker());

        spyJMSWorker.configure(getPropertiesConfig(connectorConfigProps));
        spyDedicated.configure(getPropertiesConfig(connectorConfigProps));
        spyShared.configure(getPropertiesConfig(connectorConfigProps));

        Message messageSpy = Mockito.spy(getJmsContext().createTextMessage("Spy Injected Message"));

        doReturn("1")
                .when(messageSpy)
                .getJMSMessageID();

        doReturn(messageSpy)
                .when(spyJMSWorker).receive(
                        anyString(),
                        any(QueueConfig.class),
                        anyBoolean());

        connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(connectorConfigProps, spyJMSWorker, spyDedicated, new SequenceStateClient(DEFAULT_STATE_QUEUE, spyShared, spyJMSWorker));

        String[] msgIds = new String[] {"1", "2"};

        assertThatNoException()
                .isThrownBy(() -> connectTask.removeDeliveredMessagesFromSourceQueue(Arrays.asList(msgIds)));
    }


    @Test
    public void testConfigureClientReconnectOptions() throws Exception {
        // setup test condition: put messages on source queue, poll once to read them
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.client.reconnect.options", "QMGR");

        JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(connectorConfigProps));
        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));
        SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);

        connectTask.start(connectorConfigProps, shared, dedicated, sequenceStateClient);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 2, "message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        connectTask.poll();

        List<Message> stateMsgs1 = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs1.size()).isEqualTo(1);
        shared.attemptRollback();
        assertThat(stateMsgs1.size()).isEqualTo(1); //state message is still there even though source message were rolled back

    }

    @Test
    public void verifyEmptyMessage() throws Exception {
        connectTask = new MQSourceTask();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        Message emptyMessage = getJmsContext().createMessage();
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(emptyMessage));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertNull(kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage);
    }

    @Test
    public void verifyEmptyTextMessage() throws Exception {
        connectTask = new MQSourceTask();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        TextMessage emptyMessage = getJmsContext().createTextMessage();
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(emptyMessage));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertNull(kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage);
    }

    @Test
    public void testJmsWorkerWithCustomReciveForConsumerAndCustomReconnectValues() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.message.receive.timeout", "5000");
        connectorConfigProps.put("mq.reconnect.delay.min.ms", "100");
        connectorConfigProps.put("mq.reconnect.delay.max.ms", "10000");

        final JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(connectorConfigProps));
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));
        final SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);

        connectTask.start(connectorConfigProps, shared, dedicated, sequenceStateClient);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 2, "message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        connectTask.poll();

        final List<Message> stateMsgs1 = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs1.size()).isEqualTo(1);
        shared.attemptRollback();
        assertThat(stateMsgs1.size()).isEqualTo(1);

        assertEquals(5000L, shared.getReceiveTimeout());
        assertEquals(100L, shared.getReconnectDelayMillisMin());
        assertEquals(10000L, shared.getReconnectDelayMillisMax());
    }
}
