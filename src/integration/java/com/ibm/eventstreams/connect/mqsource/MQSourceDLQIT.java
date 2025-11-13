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
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.putAllMessagesToQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;

public class MQSourceDLQIT extends AbstractJMSContextIT {

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
    public void shouldRoutePoisonMessagesToDeadLetterQueueWhenErrorToleranceIsAll_ByteMessage() throws Exception {
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
        final List<Message> testMessages = new ArrayList<>();

        final BytesMessage message1 = getJmsContext().createBytesMessage();
        message1.writeBytes("Invalid JSON message".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message1);

        final BytesMessage message2 = getJmsContext().createBytesMessage();
        message2.writeBytes("{ \"i\": 0 }".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message2);
        
        final BytesMessage message3 = getJmsContext().createBytesMessage();
        message3.writeBytes("{ \"i\": 1 }".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message3);
        
        final BytesMessage message4 = getJmsContext().createBytesMessage();
        message4.writeBytes("{ \"i\": 2 }".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message4);
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
    public void shouldRoutePoisonMessagesToDeadLetterQueueWhenErrorToleranceIsAll_JMSDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "false");
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
        final String dlqValue = new String((byte[]) poisonRecord.value(), StandardCharsets.UTF_8);
        assertThat(dlqValue.endsWith("Invalid JSON message")).isTrue();

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
    public void shouldRoutePoisonMessagesToDeadLetterQueueWhenErrorToleranceIsAll_ByteMessage_JMSDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "false");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        connectTask.start(connectorConfigProps);

        // When: Both invalid and valid messages are received
        final List<Message> testMessages = new ArrayList<>();

        final BytesMessage message1 = getJmsContext().createBytesMessage();
        message1.writeBytes("Invalid JSON message".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message1);

        final BytesMessage message2 = getJmsContext().createBytesMessage();
        message2.writeBytes("{ \"i\": 0 }".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message2);
        
        final BytesMessage message3 = getJmsContext().createBytesMessage();
        message3.writeBytes("{ \"i\": 1 }".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message3);
        
        final BytesMessage message4 = getJmsContext().createBytesMessage();
        message4.writeBytes("{ \"i\": 2 }".getBytes(StandardCharsets.UTF_8));
        testMessages.add(message4);
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();
        assertThat(processedRecords).hasSize(4);

        // Verify poison message goes to DLQ
        final SourceRecord poisonRecord = processedRecords.get(0);
        assertThat(poisonRecord.topic()).isEqualTo("__dlq.mq.source");
        assertThat(poisonRecord.valueSchema().type()).isEqualTo(Schema.Type.BYTES);
        final String dlqValue = new String((byte[]) poisonRecord.value(), StandardCharsets.UTF_8);
        assertThat(dlqValue.endsWith("Invalid JSON message"));

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
    public void verifyHeadersWithErrorTolerance_WithDLQHeaderContextDisabled_JMSBodyDisabled() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "false");
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
        final String dlqValue = new String((byte[]) dlqRecord.value(), StandardCharsets.UTF_8);
        assertThat(dlqValue.endsWith("Invalid JSON message")).isTrue();

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
    public void verifyHeadersWithErrorTolerance_WithDLQHeaderContextDisabled_JMSBodyDisabled_ByteMessage() throws Exception {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_TOPIC, "mytopic");
        connectorConfigProps.put(ConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
        connectorConfigProps.put(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG, "__dlq.mq.source");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "false");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER,
                "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        connectorConfigProps.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");

        connectTask.start(connectorConfigProps);

        final BytesMessage message1 = getJmsContext().createBytesMessage();
        final BytesMessage message2 = getJmsContext().createBytesMessage();
        message1.writeBytes("Invalid JSON message".getBytes(StandardCharsets.UTF_8));
        message2.writeBytes("{ \"i\": 0 }".getBytes(StandardCharsets.UTF_8));

        message1.setStringProperty("teststring", "myvalue");
        message1.setIntProperty("volume", 11);
        message1.setDoubleProperty("decimalmeaning", 42.0);

        // Both invalid and valid messages are received
        final List<Message> testMessages = Arrays.asList(
                message1, // Poison message
                message2 // Valid message
        );
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, testMessages);

        final List<SourceRecord> processedRecords = connectTask.poll();

        assertThat(processedRecords).hasSize(2);

        final SourceRecord dlqRecord = processedRecords.get(0);
        assertThat(dlqRecord.topic()).isEqualTo("__dlq.mq.source");

        final Headers headers = dlqRecord.headers();
        final String dlqValue = new String((byte[]) dlqRecord.value(), StandardCharsets.UTF_8);
        assertThat(dlqValue.endsWith("Invalid JSON message")).isTrue();

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
}
