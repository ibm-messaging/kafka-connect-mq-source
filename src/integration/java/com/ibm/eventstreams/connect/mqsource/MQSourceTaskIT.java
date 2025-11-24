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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.ConnectorConfig;
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

    // verify that user can use the standard approach for the JsonConverter
    //  of embedding schemas in message payloads (enabling this using a
    //  record builder config option)
    @Test
    public void verifyJmsSchemaMessages() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        connectorConfigProps.put("mq.record.builder.json.schemas.enable", "true");

        connectTask.start(connectorConfigProps);

        final List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messages.add(getJmsContext().createTextMessage(
                "{\n" +
                        "\"schema\": {\n" +
                        "    \"type\": \"struct\", \n" +
                        "    \"fields\": [\n" +
                        "        {\n" +
                        "            \"field\": \"idx\", \n" +
                        "            \"type\": \"int64\"\n" +
                        "        },\n" +
                        "        {\n" +
                        "            \"field\": \"test\", \n" +
                        "            \"type\": \"string\"\n" +
                        "        }" +
                        "    ]\n" +
                        "}, " +
                        "\"payload\": { " +
                        "    \"idx\": " + i + ", " +
                        "    \"test\" : \"abcdef\" " +
                        "}" +
                "}"));
        }
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(5, kafkaMessages.size());

        for (int i = 0; i < 5; i++) {
            final SourceRecord kafkaMessage = kafkaMessages.get(i);
            assertNull(kafkaMessage.key());

            assertNotNull(kafkaMessage.valueSchema());
            assertEquals(Schema.INT64_SCHEMA, kafkaMessage.valueSchema().field("idx").schema());
            assertEquals(Schema.STRING_SCHEMA, kafkaMessage.valueSchema().field("test").schema());

            final Struct value = (Struct) kafkaMessage.value();
            assertEquals(Long.valueOf(i), value.getInt64("idx"));
            assertEquals("abcdef", value.getString("test"));

            connectTask.commitRecord(kafkaMessage, null);
        }
    }

    // verify that a reusable schema can be provided to the JSON record builder
    //  as part of the connector config, so that this can be reused across
    //  multiple MQ messages
    @Test
    public void verifyJmsReusableSchemaMessages() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final String SCHEMA = "{\n" +
                "    \"type\": \"struct\", \n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"field\": \"idx\", \n" +
                "            \"type\": \"int32\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"a\", \n" +
                "            \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"b\", \n" +
                "            \"type\": \"int64\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"c\", \n" +
                "            \"type\": \"double\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"d\", \n" +
                "            \"type\": \"boolean\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"e\", \n" +
                "            \"type\": \"float\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"f\", \n" +
                "            \"type\": \"array\",\n" +
                "            \"items\": {\n" +
                "                \"type\": \"string\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"g\", \n" +
                "            \"type\": \"array\", \n" +
                "            \"items\": {\n" +
                "                \"type\": \"int32\"\n" +
                "            }\n" +
                "        },\n" +
                "        {\n" +
                "            \"field\": \"h\", \n" +
                "            \"type\": \"struct\", \n" +
                "            \"fields\": [\n" +
                "                {\n" +
                "                    \"field\": \"innerstr\", \n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"field\": \"innernum\", \n" +
                "                    \"type\": \"int64\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        connectorConfigProps.put("mq.record.builder.json.schemas.enable", "true");
        connectorConfigProps.put("mq.record.builder.json.schema.content", SCHEMA);

        connectTask.start(connectorConfigProps);

        final List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            messages.add(getJmsContext().createTextMessage(
                "{ " +
                    "\"idx\": " + i + ", \n" +
                    "\"a\" : \"test\", \n" +
                    "\"b\" : 1234, \n" +
                    "\"c\" : 5.67, \n" +
                    "\"d\" : false, \n" +
                    "\"e\" : 12.34, \n" +
                    "\"f\" : [ \"a\", \"b\", \"c\" ], \n" +
                    "\"g\" : [ 1, 2, 3 ], \n" +
                    "\"h\" : { \"innerstr\" : \"testing\", \"innernum\" : 89 }" +
                "}"));
        }
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, messages);

        final List<SourceRecord> kafkaMessages = connectTask.poll();
        assertEquals(5, kafkaMessages.size());

        for (int i = 0; i < 5; i++) {
            final SourceRecord kafkaMessage = kafkaMessages.get(i);
            assertNull(kafkaMessage.key());

            assertNotNull(kafkaMessage.valueSchema());
            assertEquals(Schema.INT32_SCHEMA, kafkaMessage.valueSchema().field("idx").schema());
            assertEquals(Schema.STRING_SCHEMA, kafkaMessage.valueSchema().field("a").schema());
            assertEquals(Schema.INT64_SCHEMA, kafkaMessage.valueSchema().field("b").schema());
            assertEquals(Schema.FLOAT64_SCHEMA, kafkaMessage.valueSchema().field("c").schema());
            assertEquals(Schema.BOOLEAN_SCHEMA, kafkaMessage.valueSchema().field("d").schema());
            assertEquals(Schema.FLOAT32_SCHEMA, kafkaMessage.valueSchema().field("e").schema());
            assertEquals(Schema.STRING_SCHEMA, kafkaMessage.valueSchema().field("f").schema().valueSchema());
            assertEquals(Schema.INT32_SCHEMA, kafkaMessage.valueSchema().field("g").schema().valueSchema());
            assertEquals(Schema.STRING_SCHEMA, kafkaMessage.valueSchema().field("h").schema().field("innerstr").schema());
            assertEquals(Schema.INT64_SCHEMA, kafkaMessage.valueSchema().field("h").schema().field("innernum").schema());

            final Struct value = (Struct) kafkaMessage.value();
            assertEquals(Integer.valueOf(i), value.getInt32("idx"));
            assertEquals("test", value.getString("a"));
            assertEquals(Long.valueOf(1234), value.getInt64("b"));
            assertEquals(Double.valueOf(5.67), value.getFloat64("c"));
            assertEquals(false, value.getBoolean("d"));
            assertEquals(Float.valueOf(12.34f), value.getFloat32("e"));
            assertArrayEquals(new String[]{ "a", "b", "c"}, value.getArray("f").toArray(new String[]{}));
            assertArrayEquals(new Integer[] { 1, 2, 3 }, value.getArray("g").toArray(new Integer[]{}));
            assertEquals("testing", value.getStruct("h").getString("innerstr"));
            assertEquals(Long.valueOf(89), value.getStruct("h").getInt64("innernum"));

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

        assertEquals("testmessagewithcorrelbytes", kafkaMessage.value());

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
    public void verifyErrorToleranceMessages() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("Invalid JSON message"), // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 1 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 2 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        // All messages are processed, with poison message routed to DLQ
        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(4);
        assertThat(processedRecords.stream().filter(record -> record != null)).hasSize(3);

        final List<SourceRecord> nonNullProcesssedRecord = processedRecords.stream().filter(record -> record != null)
                .collect(Collectors.toList());

        for (int i = 0; i < 3; i++) {
            final SourceRecord validRecord = nonNullProcesssedRecord.get(i);
            assertThat(validRecord.topic()).isEqualTo("mytopic");
            assertThat(validRecord.valueSchema()).isNull();

            final Map<?, ?> value = (Map<?, ?>) validRecord.value();
            assertThat(value.get("i")).isEqualTo(Long.valueOf(i));

            connectTask.commitRecord(validRecord, null);
        }
    }

    @Test
    public void shouldRoutePoisonMessagesToDeadLetterQueueWhenErrorToleranceIsAll() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // When: Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("Invalid JSON message"), // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 1 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 2 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(4);

        // Verify poison message goes to DLQ
        final SourceRecord poisonRecord = processedRecords.get(0);
        assertThat(poisonRecord.topic()).isEqualTo("__dlq.mq.source");
        assertThat(poisonRecord.valueSchema().type()).isEqualTo(Schema.Type.BYTES);
        assertThat(poisonRecord.value()).isEqualTo("Invalid JSON message".getBytes(StandardCharsets.UTF_8));

        // Verify valid messages are processed correctly
        for (int i = 1; i < 4; i++) {
            final SourceRecord validRecord = processedRecords.get(i);
            assertThat(validRecord.topic()).isEqualTo("mytopic");
            assertThat(validRecord.valueSchema()).isNull();

            final Map<?, ?> value = (Map<?, ?>) validRecord.value();
            assertThat(value.get("i")).isEqualTo(Long.valueOf(i - 1));

            connectTask.commitRecord(validRecord, null);
        }
    }

    @Test
    public void shouldFailWhenErrorToleranceIsNone() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "none");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("Invalid JSON message"), // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        // Processing should fail on the poison message
        assertThatThrownBy(() -> connectTask.poll())
                .isInstanceOfAny(ConnectException.class, RuntimeException.class)
                .hasMessageContaining("Converting byte[] to Kafka Connect data failed due to serialization error:");
    }

    @Test
    public void shouldPreserveDlqHeadersWithErrorInformation() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // An invalid message is received
        final TextMessage message = getJmsContext().createTextMessage("Invalid JSON message");
        message.setJMSMessageID("message_id");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE,
                Collections.singletonList(message));

        // The message should be routed to DLQ with error headers
        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);

        final SourceRecord dlqRecord = processedRecords.get(0);
        assertThat(dlqRecord.topic()).isEqualTo("__dlq.mq.source");

        // Verify error headers are present
        final Headers headers = dlqRecord.headers();
        assertThat(headers.lastWithName("__connect.errors.topic").value())
                .isEqualTo("mytopic");
        assertThat(headers.lastWithName("__connect.errors.exception.class.name").value())
                .isEqualTo("org.apache.kafka.connect.errors.DataException");
        assertThat(headers.lastWithName("__connect.errors.exception.message").value())
                .isEqualTo("Converting byte[] to Kafka Connect data failed due to serialization error: ");
        assertThat(headers.lastWithName("__connect.errors.timestamp").value().toString()
                .isEmpty()).isFalse();
        assertThat(headers.lastWithName("__connect.errors.cause.message").value().toString())
                .contains(
                        "com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'Invalid': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')");
        assertThat(headers.lastWithName("__connect.errors.cause.class").value())
                .isEqualTo("org.apache.kafka.common.errors.SerializationException");
        assertThat(headers.lastWithName("__connect.errors.exception.stacktrace").value()
                .toString().contains("com.ibm.eventstreams.connect.mqsource.JMSWorker.toSourceRecord")).isTrue();
        assertEquals(headers.lastWithName("__connect.errors.jms.message.id").value(), message.getJMSMessageID());
        assertEquals(headers.lastWithName("__connect.errors.jms.timestamp").value(), message.getJMSTimestamp());
        assertEquals(headers.lastWithName("__connect.errors.mq.queue").value(), DEFAULT_SOURCE_QUEUE);
        connectTask.commitRecord(dlqRecord, null);
    }

    @Test
    public void shouldHandleDifferentMessageTypesToDlq() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // When different types of invalid messages are received
        final List<Message> testMessages = new ArrayList<>();
        // Text message with invalid JSON
        testMessages.add(getJmsContext().createTextMessage("Invalid JSON message"));
        // BytesMessage with invalid content
        final BytesMessage bytesMsg = getJmsContext().createBytesMessage();
        bytesMsg.writeBytes("Invalid binary data".getBytes(StandardCharsets.UTF_8));
        testMessages.add(bytesMsg);

        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(2);

        for (final SourceRecord dlqRecord : processedRecords) {
            assertThat(dlqRecord.topic()).isEqualTo("__dlq.mq.source");
            assertThat(dlqRecord.valueSchema().type()).isEqualTo(Schema.Type.BYTES);
            connectTask.commitRecord(dlqRecord, null);
        }
    }

    @Test
    public void shouldPreserveJmsPropertiesInDlqMessages() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        final TextMessage invalidMessage = getJmsContext().createTextMessage("Invalid JSON message");
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Collections.singletonList(invalidMessage));

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(1);

        final SourceRecord dlqRecord = processedRecords.get(0);
        assertThat(dlqRecord.topic()).isEqualTo("__dlq.mq.source");

        final Headers headers = dlqRecord.headers();
        assertThat(headers.lastWithName("__connect.errors.exception.message").value())
                .isEqualTo("Converting byte[] to Kafka Connect data failed due to serialization error: ");

        connectTask.commitRecord(dlqRecord, null);
    }

    @Test
    public void shouldHandleMixOfValidAndInvalidMessagesWithDifferentFormats() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // When: Mix of valid and invalid messages received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("Invalid JSON message"), // Invalid JSON
                getJmsContext().createTextMessage("{ \"i\": 0 }"), // Valid JSON
                getJmsContext().createTextMessage("{ malformed json"), // Malformed JSON
                getJmsContext().createTextMessage("{ \"i\": 1, \"text\": \"valid\" }"), // Valid JSON
                getJmsContext().createTextMessage("{}") // Valid but empty JSON
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(5);

        int validCount = 0;
        int dlqCount = 0;

        for (final SourceRecord record : processedRecords) {
            if (record.topic().equals("__dlq.mq.source")) {
                dlqCount++;
                assertThat(record.valueSchema().type()).isEqualTo(Schema.Type.BYTES);
            } else {
                validCount++;
                assertThat(record.topic()).isEqualTo("mytopic");
            }
            connectTask.commitRecord(record, null);
        }

        assertThat(validCount).isEqualTo(3);
        assertThat(dlqCount).isEqualTo(2);
    }

    @Test
    public void shouldContinueProcessingAfterUnhandleableDlqError() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // When: Multiple messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("First invalid message"), // Invalid message
                getJmsContext().createTextMessage("{ \"i\": 0 }"), // Valid message
                getJmsContext().createTextMessage("Second invalid message"), // Invalid message
                getJmsContext().createTextMessage("{ \"i\": 1 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        // Then: Processing should continue despite DLQ failure
        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(4);

        assertThat(processedRecords.get(0).topic()).isEqualTo("__dlq.mq.source");
        assertThat(processedRecords.get(1).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(2).topic()).isEqualTo("__dlq.mq.source");
        assertThat(processedRecords.get(3).topic()).isEqualTo("mytopic");
    }

    @Test
    public void verifyHeadersWithErrorTolerance() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("Invalid JSON message");
        message.setStringProperty("teststring", "myvalue");
        message.setIntProperty("volume", 11);
        message.setDoubleProperty("decimalmeaning", 42.0);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                message, // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(2);

        assertThat(processedRecords.get(0).topic()).isEqualTo("__dlq.mq.source");

        final Headers headers = processedRecords.get(0).headers();

        // Actual headers
        assertThat(headers.lastWithName("teststring").value()).isEqualTo("myvalue");
        assertThat(headers.lastWithName("volume").value()).isEqualTo("11");
        assertThat(headers.lastWithName("decimalmeaning").value()).isEqualTo("42.0");

        // Expected DLQ Headers
        /**
        * ConnectHeaders(headers=[ConnectHeader(key=__connect.errors.topic, value=mytopic, schema=Schema{STRING}),
        * ConnectHeader(key=__connect.errors.class.name, value=org.apache.kafka.connect.errors.DataException, schema=Schema{STRING}),
        * ConnectHeader(key=__connect.errors.exception.message, value=Converting byte[] to Kafka Connect data failed due to serialization error: , schema=Schema{STRING}),
        * ConnectHeader(key=__connect.errors.timestamp, value=1749036171558, schema=Schema{STRING}),
        * ConnectHeader(key=__connect.errors.cause.message, value=com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'Invalid': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
        *     at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 9], schema=Schema{STRING}),
        * ConnectHeader(key=__connect.errors.cause.class, value=org.apache.kafka.common.errors.SerializationException, schema=Schema{STRING}),
        * ConnectHeader(key=__connect.errors.exception.stacktrace, value=org.apache.kafka.connect.errors.DataException:
        *     Converting byte[] to Kafka Connect data failed due to serialization error:
        *     at org.apache.kafka.connect.json.JsonConverter.toConnectData(JsonConverter.java:333)
        *     at com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder.getValue(JsonRecordBuilder.java:81)
        *     at com.ibm.eventstreams.connect.mqsource.builders.BaseRecordBuilder.toSourceRecord(BaseRecordBuilder.java:238)
        *     at com.ibm.eventstreams.connect.mqsource.JMSWorker.toSourceRecord(JMSWork... [truncated], schema=Schema{STRING})])
        */
        assertThat(headers.lastWithName("__connect.errors.topic").value())
                .isEqualTo("mytopic");
        assertThat(headers.lastWithName("__connect.errors.exception.class.name").value())
                .isEqualTo("org.apache.kafka.connect.errors.DataException");
        assertThat(headers.lastWithName("__connect.errors.exception.message").value())
                .isEqualTo("Converting byte[] to Kafka Connect data failed due to serialization error: ");
        assertThat(headers.lastWithName("__connect.errors.timestamp").value().toString()
                .isEmpty()).isFalse();
        assertThat(headers.lastWithName("__connect.errors.cause.message").value().toString())
                .contains(
                        "com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'Invalid': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')");
        assertThat(headers.lastWithName("__connect.errors.cause.class").value())
                .isEqualTo("org.apache.kafka.common.errors.SerializationException");
        assertThat(headers.lastWithName("__connect.errors.exception.stacktrace").value()
                .toString().contains("com.ibm.eventstreams.connect.mqsource.JMSWorker.toSourceRecord")).isTrue();
    }

    @Test
    public void verifyHeadersWithErrorTolerance_WithDLQHeaderContextDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");

        connectTask.start(connectorConfigProps);

        final TextMessage message = getJmsContext().createTextMessage("Invalid JSON message");
        message.setStringProperty("teststring", "myvalue");
        message.setIntProperty("volume", 11);
        message.setDoubleProperty("decimalmeaning", 42.0);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                message, // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(2);

        final SourceRecord dlqRecord = processedRecords.get(0);
        assertThat(dlqRecord.topic()).isEqualTo("__dlq.mq.source");

        final Headers headers = dlqRecord.headers();

        // Actual headers
        assertThat(headers.lastWithName("teststring").value()).isEqualTo("myvalue");
        assertThat(headers.lastWithName("volume").value()).isEqualTo("11");
        assertThat(headers.lastWithName("decimalmeaning").value()).isEqualTo("42.0");

        assertThat(headers.lastWithName("__connect.errors.topic")).isNull();
        assertThat(headers.lastWithName("__connect.errors.class.name")).isNull();
        assertThat(headers.lastWithName("__connect.errors.exception.message")).isNull();
        assertThat(headers.lastWithName("__connect.errors.timestamp")).isNull();
        assertThat(headers.lastWithName("__connect.errors.cause.message")).isNull();
        assertThat(headers.lastWithName("__connect.errors.cause.class")).isNull();
        assertThat(headers.lastWithName("__connect.errors.exception.stacktrace")).isNull();
    }

    @Test
    public void verifyLoggingWarningWithErrorTolerance() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "false"); // default; Do not log errors
        // default; Do not log errors with message
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "false");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("Invalid JSON message"), // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 1 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 2 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(4);

        assertThat(processedRecords.get(0).topic()).isEqualTo("__dlq.mq.source");
        assertThat(processedRecords.get(1).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(2).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(3).topic()).isEqualTo("mytopic");
    }

    @Test
    public void verifyLoggingErrorsWithErrorTolerance() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true"); // Log errors enabled
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "false");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("Invalid JSON message"), // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 1 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 2 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(4);

        assertThat(processedRecords.get(0).topic()).isEqualTo("__dlq.mq.source");
        assertThat(processedRecords.get(1).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(2).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(3).topic()).isEqualTo("mytopic");
    }

    @Test
    public void verifyLoggingErrorsWithMessage() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true"); // Log errors
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true"); // Log errors with message
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createTextMessage("Invalid JSON message"), // Poison message
                getJmsContext().createTextMessage("{ \"i\": 0 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 1 }"), // Valid message
                getJmsContext().createTextMessage("{ \"i\": 2 }") // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(4);

        assertThat(processedRecords.get(0).topic()).isEqualTo("__dlq.mq.source");
        assertThat(processedRecords.get(1).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(2).topic()).isEqualTo("mytopic");
        assertThat(processedRecords.get(3).topic()).isEqualTo("mytopic");
    }

    @Test
    public void verifyLoggingErrorsWithMessageHavingDefaultRecordBuilder() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true"); // Log errors
        connectorConfigProps.put(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG, "true"); // Log errors with message
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        connectorConfigProps.put("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
        connectTask.start(connectorConfigProps);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                getJmsContext().createObjectMessage("Invalid message"), // Poison message
                getJmsContext().createTextMessage("Text") // Valid
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(2);

        assertThat(processedRecords.get(0).topic()).isEqualTo("__dlq.mq.source");
        assertThat(processedRecords.get(1).topic()).isEqualTo("mytopic");
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
}
