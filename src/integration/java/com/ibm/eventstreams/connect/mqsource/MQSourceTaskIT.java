/**
 * Copyright 2022, 2023, 2024, 2025 IBM Corporation
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
import org.apache.kafka.connect.header.Headers;
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
        props.put("mq.message.receive.timeout", "5000");
        props.put("mq.receive.subsequent.timeout.ms", "2000");
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
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

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

            connectTask.commitRecord(kafkaMessage, null);
        }

        assertEquals("hello", kafkaMessages.get(0).value());
        assertEquals("world", kafkaMessages.get(1).value());
    }

    @Test
    public void verifyJmsJsonMessages() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

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

            connectTask.commitRecord(kafkaMessage, null);
        }
    }

    @Test
    public void verifyMQMessage() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "false"); // this could also be absent but if set to true the
                                                                  // test should fail
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final String sent = "Hello World";
        final String url = "https://localhost:" + REST_API_HOST_PORT + "/ibmmq/rest/v1/messaging/qmgr/" + QMGR_NAME
                + "/queue/DEV.QUEUE.1/message";
        JsonRestApi.postString(url, "app", ADMIN_PASSWORD, sent);

        final List<SourceRecord> kafkaMessages = connectTask.poll(); // get all the SRs (1)
        final SourceRecord firstMsg = kafkaMessages.get(0);
        final Object received = firstMsg.value();

        assertNotEquals(received.getClass(), String.class); // jms messages are retrieved as Strings
        assertEquals(received.getClass(), byte[].class);
        assertEquals(new String((byte[]) received, StandardCharsets.UTF_8), sent);

        connectTask.commitRecord(firstMsg, null);
        connectTask.poll();
    }

    @Test
    public void verifyJmsMessageHeaders() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
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

        connectTask.commitRecord(kafkaMessage, null);
    }

    @Test
    public void verifyMessageBatchIndividualCommits() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();

        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
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
            connectTask.commitRecord(kafkaMessage, null);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertEquals("batch message " + (nextExpectedMessage++), kafkaMessage.value());
            connectTask.commitRecord(kafkaMessage, null);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertEquals("batch message " + (nextExpectedMessage++), kafkaMessage.value());
            connectTask.commitRecord(kafkaMessage, null);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(5, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertEquals("batch message " + (nextExpectedMessage++), kafkaMessage.value());
            connectTask.commitRecord(kafkaMessage, null);
        }
    }

    @Test
    public void verifyMessageBatchGroupCommits() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.batch.size", "10");

        connectTask.start(connectorConfigProps);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 35, "message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        List<SourceRecord> kafkaMessages;

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m, null);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m, null);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(10, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m, null);
        }

        kafkaMessages = connectTask.poll();
        assertEquals(5, kafkaMessages.size());
        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m, null);
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

        connectTask.commitRecord(kafkaMessage, null);
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
        connectTask.commitRecord(kafkaMessage1, null);

        final SourceRecord kafkaMessage2 = kafkaMessages.get(1);
        assertEquals("5fb4a18030154fe4b09a1dfe8075bc101dfe8075bc104fe4", kafkaMessage2.key());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, kafkaMessage2.keySchema());
        assertEquals("second message", kafkaMessage2.value());
        connectTask.commitRecord(kafkaMessage2, null);
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
        assertThat(kafkaMessage.valueSchema()).isNull();

        assertEquals("testmessagewithcorrelbytes", kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage, null);
    }

    @Test
    public void verifyCorrelationIdBytesAsKey_WithJMSDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "false");
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
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, kafkaMessage.valueSchema());

        assertThat(new String((byte[]) kafkaMessage.value(), StandardCharsets.UTF_8).endsWith("testmessagewithcorrelbytes")).isTrue();
        connectTask.commitRecord(kafkaMessage, null);
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

        connectTask.commitRecord(kafkaMessage, null);
    }

    @Test
    public void testSequenceStateMsgReadUnderMQTx() throws Exception {
        final JMSWorker spyJMSWorker = Mockito.spy(new JMSWorker());

        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        spyJMSWorker.configure(getPropertiesConfig(connectorConfigProps));
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));
        final SequenceStateClient sequenceStateClient = Mockito
                .spy(new SequenceStateClient(DEFAULT_STATE_QUEUE, spyJMSWorker, dedicated));

        connectTask.start(connectorConfigProps, spyJMSWorker, dedicated, sequenceStateClient);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 2, "message ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        final List<SourceRecord> kafkaMessages;
        kafkaMessages = connectTask.poll();

        final List<Message> stateMsgs1 = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs1.size()).isEqualTo(1);

        for (final SourceRecord m : kafkaMessages) {
            connectTask.commitRecord(m, null);
        }

        /// make commit do rollback when poll is called
        doAnswer((Void) -> {
            spyJMSWorker.getContext().rollback();
            throw new Exception("such an exception");

        }).when(spyJMSWorker).commit();

        try {
            connectTask.poll();
        } catch (final Exception e) {
            System.out.println("exception caught");
        }

        /// expect statequeue to not be empty
        final List<Message> stateMsgs2 = getAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs2.size()).isEqualTo(1);

        final List<Message> sourceMsgs = getAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(sourceMsgs.size()).isEqualTo(2);

    }

    @Test
    public void testSequenceStateMsgWrittenIndependentFromGetSource() throws Exception {
        // setup test condition: put messages on source queue, poll once to read them
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

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
        assertThat(stateMsgs1.size()).isEqualTo(1); // state message is still there even though source message were
                                                    // rolled back

    }

    @Test
    public void testRemoveDeliveredMessagesFromSourceQueueThrowsException() throws Exception {

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        final JMSWorker spyJMSWorker = Mockito.spy(new JMSWorker());
        final JMSWorker spyDedicated = Mockito.spy(new JMSWorker());
        final JMSWorker spyShared = Mockito.spy(new JMSWorker());

        spyJMSWorker.configure(getPropertiesConfig(connectorConfigProps));
        spyDedicated.configure(getPropertiesConfig(connectorConfigProps));
        spyShared.configure(getPropertiesConfig(connectorConfigProps));

        final Message messageSpy = Mockito.spy(getJmsContext().createTextMessage("Spy Injected Message"));

        doReturn("6")
                .when(messageSpy)
                .getJMSMessageID();

        doReturn(messageSpy)
                .when(spyJMSWorker).receive(
                        anyString(),
                        any(QueueConfig.class),
                        anyBoolean());

        connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(connectorConfigProps, spyJMSWorker, spyDedicated,
                new SequenceStateClient(DEFAULT_STATE_QUEUE, spyShared, spyJMSWorker));

        final String[] msgIds = new String[] { "1", "2" };

        assertThrows(SequenceStateException.class,
                () -> connectTask.removeDeliveredMessagesFromSourceQueue(Arrays.asList(msgIds)));
    }

    @Test
    public void testRemoveDeliveredMessagesFromSourceQueueDoesNotThrowException() throws Exception {

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        final JMSWorker spyJMSWorker = Mockito.spy(new JMSWorker());
        final JMSWorker spyDedicated = Mockito.spy(new JMSWorker());
        final JMSWorker spyShared = Mockito.spy(new JMSWorker());

        spyJMSWorker.configure(getPropertiesConfig(connectorConfigProps));
        spyDedicated.configure(getPropertiesConfig(connectorConfigProps));
        spyShared.configure(getPropertiesConfig(connectorConfigProps));

        final Message messageSpy = Mockito.spy(getJmsContext().createTextMessage("Spy Injected Message"));

        doReturn("1")
                .when(messageSpy)
                .getJMSMessageID();

        doReturn(messageSpy)
                .when(spyJMSWorker).receive(
                        anyString(),
                        any(QueueConfig.class),
                        anyBoolean());

        connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(connectorConfigProps, spyJMSWorker, spyDedicated,
                new SequenceStateClient(DEFAULT_STATE_QUEUE, spyShared, spyJMSWorker));

        final String[] msgIds = new String[] { "1", "2" };

        assertThatNoException()
                .isThrownBy(() -> connectTask.removeDeliveredMessagesFromSourceQueue(Arrays.asList(msgIds)));
    }

    @Test
    public void testConfigureClientReconnectOptions() throws Exception {
        // setup test condition: put messages on source queue, poll once to read them
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.client.reconnect.options", "QMGR");

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
        assertThat(stateMsgs1.size()).isEqualTo(1); // state message is still there even though source message were
                                                    // rolled back

    }

    @Test
    public void verifyEmptyMessage() throws Exception {
        connectTask = new MQSourceTask();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final Message emptyMessage = getJmsContext().createMessage();
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(emptyMessage));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertNull(kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage, null);
    }

    @Test
    public void verifyEmptyTextMessage() throws Exception {
        connectTask = new MQSourceTask();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage emptyMessage = getJmsContext().createTextMessage();
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(emptyMessage));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);
        assertNull(kafkaMessage.value());

        connectTask.commitRecord(kafkaMessage, null);
    }

    @Test
    public void testJmsWorkerWithCustomReciveForConsumerAndCustomReconnectValues() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.message.receive.timeout", "2000");
        connectorConfigProps.put("mq.receive.subsequent.timeout.ms", "3000");
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

        assertEquals(2000L, shared.getInitialReceiveTimeoutMs());
        assertEquals(3000L, shared.getSubsequentReceiveTimeoutMs());
        assertEquals(100L, shared.getReconnectDelayMillisMin());
        assertEquals(10000L, shared.getReconnectDelayMillisMax());
    }

    @Test
    public void verifyJmsMessageWithNullHeaders() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.jms.properties.copy.to.kafka.headers", "true");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("hello");
        message.setStringProperty("teststring", "myvalue");
        message.setObjectProperty("testObject", null);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(1, kafkaMessages.size());

        final SourceRecord kafkaMessage = kafkaMessages.get(0);

        assertThat(kafkaMessage.value()).isEqualTo("hello");
        assertThat(kafkaMessage.headers().lastWithName("teststring").value()).isEqualTo("myvalue");
        assertThat(kafkaMessage.headers().lastWithName("testObject").value()).isNull();
    }

    @Test
    public void verifyJmsMessageNoHeaderCopied_WhenCopyDisabledHavingNullHeader() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.jms.properties.copy.to.kafka.headers", "false");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("hello");
        message.setStringProperty("teststring", "myvalue");
        message.setObjectProperty("testObject", null);
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final SourceRecord kafkaMessage = connectTask.poll().get(0);

        assertThat(kafkaMessage.value()).isEqualTo("hello");
        assertThat(kafkaMessage.headers()).isEmpty();
    }

    @Test
    public void testMaxPollTimeTerminates() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.message.receive.timeout", "150");
        connectorConfigProps.put("mq.receive.subsequent.timeout.ms", "10");
        connectorConfigProps.put("mq.receive.max.poll.time.ms", "200");
        connectorConfigProps.put("mq.batch.size", "5000");

        final JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(connectorConfigProps));
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));
        final SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);

        connectTask.start(connectorConfigProps, shared, dedicated, sequenceStateClient);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 10, "msg ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        final List<SourceRecord> kafkaMessages = connectTask.poll();

        final List<Message> stateMsgs1 = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs1.size()).isEqualTo(1);
        final List<Message> sourceMsgs = getAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(sourceMsgs.size()).isEqualTo(0);
        assertEquals(30, kafkaMessages.size());
    }

    @Test
    public void testMaxPollTimeTerminatesBatchEarly() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createExactlyOnceConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder",
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put("mq.message.receive.timeout", "100");
        connectorConfigProps.put("mq.receive.subsequent.timeout.ms", "10");
        connectorConfigProps.put("mq.receive.max.poll.time.ms", "200"); // stop after 200ms
        connectorConfigProps.put("mq.batch.size", "5000");

        final JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(connectorConfigProps));
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(connectorConfigProps));
        final SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);

        connectTask.start(connectorConfigProps, shared, dedicated, sequenceStateClient);

        final List<Message> messages = createAListOfMessages(getJmsContext(), 10, "msg ");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);
        final long start = System.nanoTime();
        final List<SourceRecord> kafkaMessages = connectTask.poll();
        final long durationMs = (System.nanoTime() - start) / 1_000_000;

        // Poll should end close to 200ms
        assertThat(durationMs <= 210).isTrue();

        final List<Message> stateMsgs1 = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs1.size()).isEqualTo(1);
        final List<Message> sourceMsgs = getAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        assertThat(sourceMsgs.size()).isEqualTo(0);
        assertEquals(10, kafkaMessages.size());
    }

    @Test
    public void testPollEndsWhenBatchSizeReached() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> config = createExactlyOnceConnectorProperties();
        config.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        config.put("mq.message.receive.timeout", "100");
        config.put("mq.receive.subsequent.timeout.ms", "10");
        config.put("mq.receive.max.poll.time.ms", "1000");
        config.put("mq.batch.size", "10");

        final JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(config));
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(config));
        final SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);
        connectTask.start(config, shared, dedicated, sequenceStateClient);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, createAListOfMessages(getJmsContext(), 12, "msg "));

        final long start = System.nanoTime();
        connectTask.poll();
        final long durationMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(durationMs < 1000).isTrue();
    }

    @Test
    public void testPollWithMaxPollTimeZeroBehavesAsDefault() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();
        final Map<String, String> config = createExactlyOnceConnectorProperties();
        config.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        config.put("mq.message.receive.timeout", "400");
        config.put("mq.receive.max.poll.time.ms", "0");
        config.put("mq.batch.size", "100");

        final JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(config));
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(config));
        final SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);
        connectTask.start(config, shared, dedicated, sequenceStateClient);

        // putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, createAListOfMessages(getJmsContext(), 3, "msg "));

        final long start = System.nanoTime();
        final List<SourceRecord> records = connectTask.poll();
        final long durationMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(durationMs >= 400 && durationMs <= 450).isTrue();
        assertEquals(0, records.size());
    }

    @Test
    public void testPollWithShortMaxPollTime() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();
        final Map<String, String> config = createExactlyOnceConnectorProperties();
        config.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        config.put("mq.receive.max.poll.time.ms", "50");
        config.put("mq.message.receive.timeout", "1");
        config.put("mq.receive.subsequent.timeout.ms", "0");
        config.put("mq.batch.size", "5000");

        final JMSWorker shared = new JMSWorker();
        shared.configure(getPropertiesConfig(config));
        final JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(config));
        final SequenceStateClient sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);
        connectTask.start(config, shared, dedicated, sequenceStateClient);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, createAListOfMessages(getJmsContext(), 100, "msg "));

        final List<SourceRecord> records = connectTask.poll();

        assertThat(records.size() < 100);
    }

    @Test
    public void shouldSetJmsPropertiesIFJMSIsDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "false");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("message");
        message.setStringProperty("teststring", "myvalue");
        message.setIntProperty("volume", 11);
        message.setDoubleProperty("decimalmeaning", 42.0);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);
        assertThat(processedRecords.get(0).topic()).isEqualTo("mytopic");

        final Headers headers = processedRecords.get(0).headers();

        // Actual headers
        assertThat(headers.lastWithName("teststring").value()).isEqualTo("myvalue");
        assertThat(headers.lastWithName("volume").value()).isEqualTo("11");
        assertThat(headers.lastWithName("decimalmeaning").value()).isEqualTo("42.0");
    }

    @Test
    public void shouldSetJmsPropertiesWithDefaultRecordBuilderWhenJMSIsEnabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("test message content");
        message.setStringProperty("customHeader", "headerValue");
        message.setIntProperty("priority", 5);
        message.setDoubleProperty("price", 99.99);
        message.setBooleanProperty("isActive", true);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);
        assertThat(processedRecords.get(0).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(0).value()).isEqualTo("test message content");

        final Headers headers = processedRecords.get(0).headers();

        // Verify JMS properties are copied to Kafka headers
        assertThat(headers.lastWithName("customHeader").value()).isEqualTo("headerValue");
        assertThat(headers.lastWithName("priority").value()).isEqualTo("5");
        assertThat(headers.lastWithName("price").value()).isEqualTo("99.99");
        assertThat(headers.lastWithName("isActive").value()).isEqualTo("true");
    }

    @Test
    public void shouldNotSetJmsPropertiesWithDefaultRecordBuilderWhenCopyIsDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "false");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("test message");
        message.setStringProperty("shouldNotAppear", "value");
        message.setIntProperty("count", 42);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);
        assertThat(processedRecords.get(0).value()).isEqualTo("test message");

        final Headers headers = processedRecords.get(0).headers();

        // Verify no JMS properties are copied when disabled
        assertThat(headers.lastWithName("shouldNotAppear")).isNull();
        assertThat(headers.lastWithName("count")).isNull();
    }

    @Test
    public void shouldSetJmsPropertiesWithJsonRecordBuilderWhenJMSIsEnabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("{ \"id\": 123, \"name\": \"test\" }");
        message.setStringProperty("correlationId", "corr-123");
        message.setIntProperty("retryCount", 3);
        message.setDoubleProperty("amount", 150.75);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);
        assertThat(processedRecords.get(0).topic()).isEqualTo("mytopic");

        // Verify JSON content is parsed correctly
        final Map<?, ?> value = (Map<?, ?>) processedRecords.get(0).value();
        assertThat(value.get("id")).isEqualTo(123L);
        assertThat(value.get("name")).isEqualTo("test");

        final Headers headers = processedRecords.get(0).headers();

        // Verify JMS properties are copied to Kafka headers
        assertThat(headers.lastWithName("correlationId").value()).isEqualTo("corr-123");
        assertThat(headers.lastWithName("retryCount").value()).isEqualTo("3");
        assertThat(headers.lastWithName("amount").value()).isEqualTo("150.75");
    }

    @Test
    public void shouldNotSetJmsPropertiesWithJsonRecordBuilderWhenCopyIsDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "false");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("{ \"data\": \"value\" }");
        message.setStringProperty("hiddenProperty", "shouldNotAppear");
        message.setIntProperty("hiddenCount", 99);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);

        final Headers headers = processedRecords.get(0).headers();

        // Verify no JMS properties are copied when disabled
        assertThat(headers.lastWithName("hiddenProperty")).isNull();
        assertThat(headers.lastWithName("hiddenCount")).isNull();
    }

    @Test
    public void shouldHandleMultipleJmsPropertiesWithDifferentTypesDefaultBuilder() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("multi-property message");
        message.setStringProperty("stringProp", "text");
        message.setIntProperty("intProp", 100);
        message.setLongProperty("longProp", 999999999L);
        message.setFloatProperty("floatProp", 3.14f);
        message.setDoubleProperty("doubleProp", 2.71828);
        message.setBooleanProperty("boolProp", false);
        message.setByteProperty("byteProp", (byte) 127);
        message.setShortProperty("shortProp", (short) 32000);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);

        final Headers headers = processedRecords.get(0).headers();

        // Verify all property types are correctly converted to string headers
        assertThat(headers.lastWithName("stringProp").value()).isEqualTo("text");
        assertThat(headers.lastWithName("intProp").value()).isEqualTo("100");
        assertThat(headers.lastWithName("longProp").value()).isEqualTo("999999999");
        assertThat(headers.lastWithName("floatProp").value()).isEqualTo("3.14");
        assertThat(headers.lastWithName("doubleProp").value()).isEqualTo("2.71828");
        assertThat(headers.lastWithName("boolProp").value()).isEqualTo("false");
        assertThat(headers.lastWithName("byteProp").value()).isEqualTo("127");
        assertThat(headers.lastWithName("shortProp").value()).isEqualTo("32000");
    }

    @Test
    public void shouldHandleMultipleJmsPropertiesWithDifferentTypesJsonBuilder() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("{ \"type\": \"multi-prop\" }");
        message.setStringProperty("env", "production");
        message.setIntProperty("maxRetries", 5);
        message.setLongProperty("createdAt", 1609459200000L);
        message.setDoubleProperty("threshold", 0.95);
        message.setBooleanProperty("enabled", true);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);

        final Headers headers = processedRecords.get(0).headers();

        // Verify all property types are correctly converted
        assertThat(headers.lastWithName("env").value()).isEqualTo("production");
        assertThat(headers.lastWithName("maxRetries").value()).isEqualTo("5");
        assertThat(headers.lastWithName("createdAt").value()).isEqualTo("1609459200000");
        assertThat(headers.lastWithName("threshold").value()).isEqualTo("0.95");
        assertThat(headers.lastWithName("enabled").value()).isEqualTo("true");
    }

    @Test
    public void shouldSetJmsPropertiesWithJsonRecordBuilderWhenJMSIsDisabled_InvalidJSON() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "false");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext()
                .createTextMessage("{ \"id\": 123, \"name\": \"test\", \"active\": true }");
        message.setStringProperty("source", "system-a");
        message.setIntProperty("version", 2);
        message.setLongProperty("timestamp", 1234567890L);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));

        final List<SourceRecord> processedRecords = connectTask.poll();
        SourceRecord record = processedRecords.get(0);
        assertThat(processedRecords).hasSize(1);
        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo("mytopic");
        assertThat(record.value()).isInstanceOf(Map.class);
        assertNull(record.valueSchema()); // JSON with no schema

        // Verify JSON data
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) record.value();
        assertEquals(123L, value.get("id"));
        assertEquals("test", value.get("name"));
        assertEquals(true, value.get("active"));

        final Headers headers = record.headers();

        // Verify JMS properties are copied even when JMS body processing is disabled
        assertThat(headers.lastWithName("source").value()).isEqualTo("system-a");
        assertThat(headers.lastWithName("version").value()).isEqualTo("2");
        assertThat(headers.lastWithName("timestamp").value()).isEqualTo("1234567890");
    }
}