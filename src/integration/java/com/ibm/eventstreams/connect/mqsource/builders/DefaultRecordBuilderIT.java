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
package com.ibm.eventstreams.connect.mqsource.builders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT;
import com.ibm.eventstreams.connect.mqsource.JMSWorker;

public class DefaultRecordBuilderIT extends AbstractJMSContextIT {

    private static final String TOPIC = "MY.TOPIC";


    @Test
    public void buildFromJmsMapMessage() throws Exception {
        final String messageContent = "This is the message contents";
        final boolean isJMS = true;

        // create MQ message
        final MapMessage message = getJmsContext().createMapMessage();
        message.setString("example", messageContent);

        // use the builder to convert it to a Kafka record
        final DefaultRecordBuilder builder = new DefaultRecordBuilder();
        final RecordBuilderException exc = assertThrows(RecordBuilderException.class, () -> {
            builder.toSourceRecord(getJmsContext(), TOPIC, isJMS, message);
        });

        // verify the exception
        assertEquals("Unsupported JMS message type", exc.getMessage());
    }

    @Test
    public void buildFromJmsTextMessage() throws Exception {
        final String messageContents = "This is the JMS message contents";
        final boolean isJMS = true;

        // create MQ message
        final TextMessage message = getJmsContext().createTextMessage(messageContents);

        // use the builder to convert it to a Kafka record
        final DefaultRecordBuilder builder = new DefaultRecordBuilder();
        final SourceRecord record = builder.toSourceRecord(getJmsContext(), TOPIC, isJMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertEquals(messageContents, record.value());
        assertNull(record.valueSchema());
    }

    @Test
    public void buildFromTextMessage() throws Exception {
        final String messageContents = "This is the message contents";
        final boolean isJMS = false;

        // create MQ message
        final TextMessage message = getJmsContext().createTextMessage(messageContents);

        // use the builder to convert it to a Kafka record
        final DefaultRecordBuilder builder = new DefaultRecordBuilder();
        final MessageFormatException exc = assertThrows(MessageFormatException.class, () -> {
            builder.toSourceRecord(getJmsContext(), TOPIC, isJMS, message);
        });

        // verify the exception
        assertEquals("JMSCC5002", exc.getErrorCode());
        assertTrue(exc.getMessage().contains("The message of type jms_text can not have its body assigned to"));
    }

    @Test
    public void buildFromJmsBytesMessage() throws Exception {
        final String messageOrigin = "This is the data used for message contents";
        final byte[] messageContents = messageOrigin.getBytes();
        final boolean isJMS = true;

        // create MQ message
        final BytesMessage message = getJmsContext().createBytesMessage();
        message.writeBytes(messageContents);
        message.reset();

        // use the builder to convert it to a Kafka record
        final DefaultRecordBuilder builder = new DefaultRecordBuilder();
        final SourceRecord record = builder.toSourceRecord(getJmsContext(), TOPIC, isJMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertArrayEquals(messageContents, (byte[]) record.value());
        assertNull(record.valueSchema());
    }

    @Test
    public void buildFromBytesMessage() throws Exception {
        final String messageOrigin = "This is the data used for message contents";
        final byte[] messageContents = messageOrigin.getBytes();
        final boolean isJMS = false;

        // create MQ message
        final BytesMessage message = getJmsContext().createBytesMessage();
        message.writeBytes(messageContents);
        message.reset();

        // use the builder to convert it to a Kafka record
        final DefaultRecordBuilder builder = new DefaultRecordBuilder();
        final SourceRecord record = builder.toSourceRecord(getJmsContext(), TOPIC, isJMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertArrayEquals(messageContents, (byte[]) record.value());
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, record.valueSchema());
    }

    @Test
    public void testBuildWithOffset() throws Exception {
        final String messageContents = "This is the JMS message contents";
        final boolean isJMS = true;

        // create MQ message
        final TextMessage message = getJmsContext().createTextMessage(messageContents);

        // use the builder to convert it to a Kafka record
        final DefaultRecordBuilder builder = new DefaultRecordBuilder();

        Map<String, Long> sourceOffset = new HashMap<>();
        sourceOffset.put("sequence-id", 0L);

        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("source", "myqmgr/myq");


        final SourceRecord record = builder.toSourceRecord(getJmsContext(), TOPIC, isJMS, message, sourceOffset, sourcePartition);

        assertThat(record).isNotNull();
        assertThat(record.sourceOffset()).isEqualTo(sourceOffset);
        assertThat(record.sourcePartition()).isEqualTo(sourcePartition);

        // verify the Kafka record
        assertNull(record.key());
        assertEquals(messageContents, record.value());
        assertNull(record.valueSchema());
    }

    @Test
    public void testToSourceRecord_DefaultRecordBuilder_AnyMessage_JmsFalse_ByteArrayConverter() throws Exception {
        // Test: DefaultRecordBuilder with mq.message.body.jms=false
        // Expected: String data output
        Map<String, String> connectorProps = getDefaultConnectorProperties();
        connectorProps.put("mq.message.body.jms", "false");
        connectorProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorProps.put("mq.jms.properties.copy.to.kafka.headers", "true");

        JMSWorker worker = new JMSWorker();
        worker.configure(getPropertiesConfig(connectorProps));
        worker.connect();

        try {
            TextMessage textMessage = getJmsContext().createTextMessage("Text message");
            textMessage.setStringProperty("customHeader", "headerValue");
            textMessage.setIntProperty("priority", 5);
            textMessage.setDoubleProperty("price", 99.99);

            Map<String, Long> sourceOffset = new HashMap<>();
            sourceOffset.put("sequence-id", 1L);

            Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put("source", "myqmgr/myq");

            SourceRecord sourceRecord = worker.toSourceRecord(textMessage, true, sourceOffset, sourcePartition);

            assertThat(sourceRecord).isNotNull();
            assertThat(sourceRecord.value()).isInstanceOf(String.class);
            assertThat(sourceRecord.valueSchema()).isNull();
            
            // Verify data
            String value = (String) sourceRecord.value();
            assertThat(value).isNotNull();

            // Verify JMS properties are copied to Kafka headers
            Headers headers = sourceRecord.headers();
            assertThat(headers.lastWithName("customHeader").value()).isEqualTo("headerValue");
            assertThat(headers.lastWithName("priority").value()).isEqualTo("5");
            assertThat(headers.lastWithName("price").value()).isEqualTo("99.99");
        } finally {
            worker.stop();
        }
    }

    @Test
    public void testToSourceRecord_DefaultRecordBuilder_BytesMessage_JmsTrue() throws Exception {
        // Test: DefaultRecordBuilder with JMS BytesMessage, mq.message.body.jms=true
        // Expected: Binary data output
        Map<String, String> connectorProps = getDefaultConnectorProperties();
        connectorProps.put("mq.message.body.jms", "true");
        connectorProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorProps.put("mq.jms.properties.copy.to.kafka.headers", "true");

        JMSWorker worker = new JMSWorker();
        worker.configure(getPropertiesConfig(connectorProps));
        worker.connect();

        try {
            BytesMessage bytesMessage = getJmsContext().createBytesMessage();
            byte[] testData = "Binary bytes message".getBytes(StandardCharsets.UTF_8);
            bytesMessage.writeBytes(testData);
            bytesMessage.setStringProperty("messageType", "binary");
            bytesMessage.setIntProperty("version", 2);
            bytesMessage.setLongProperty("timestamp", 1234567890L);
            bytesMessage.reset(); // Reset to make the message readable

            Map<String, Long> sourceOffset = new HashMap<>();
            sourceOffset.put("sequence-id", 2L);

            Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put("source", "myqmgr/myq");

            SourceRecord sourceRecord = worker.toSourceRecord(bytesMessage, true, sourceOffset, sourcePartition);

            assertThat(sourceRecord).isNotNull();
            assertThat(sourceRecord.value()).isInstanceOf(byte[].class);
            assertThat(sourceRecord.valueSchema()).isNull();
            
            // Verify binary data matches
            byte[] value = (byte[]) sourceRecord.value();
            assertArrayEquals(testData, value);

            // Verify JMS properties are copied to Kafka headers
            Headers headers = sourceRecord.headers();
            assertThat(headers.lastWithName("messageType").value()).isEqualTo("binary");
            assertThat(headers.lastWithName("version").value()).isEqualTo("2");
            assertThat(headers.lastWithName("timestamp").value()).isEqualTo("1234567890");
        } finally {
            worker.stop();
        }
    }

    @Test
    public void testToSourceRecord_DefaultRecordBuilder_TextMessage_JmsTrue() throws Exception {
        // Test: DefaultRecordBuilder with JMS TextMessage, mq.message.body.jms=true and StringConverter
        // Expected: String data output
        Map<String, String> connectorProps = getDefaultConnectorProperties();
        connectorProps.put("mq.message.body.jms", "true");
        connectorProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorProps.put("mq.jms.properties.copy.to.kafka.headers", "true");

        JMSWorker worker = new JMSWorker();
        worker.configure(getPropertiesConfig(connectorProps));
        worker.connect();

        try {
            String testText = "This is a text message";
            TextMessage textMessage = getJmsContext().createTextMessage(testText);
            textMessage.setStringProperty("source", "system-a");
            textMessage.setIntProperty("retryCount", 3);
            textMessage.setDoubleProperty("threshold", 0.95);
            textMessage.setBooleanProperty("enabled", true);

            Map<String, Long> sourceOffset = new HashMap<>();
            sourceOffset.put("sequence-id", 3L);

            Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put("source", "myqmgr/myq");

            SourceRecord sourceRecord = worker.toSourceRecord(textMessage, true, sourceOffset, sourcePartition);

            assertThat(sourceRecord).isNotNull();
            assertThat(sourceRecord.value()).isInstanceOf(String.class);
            assertNull(sourceRecord.valueSchema());
            
            // Verify string data matches
            String value = (String) sourceRecord.value();
            assertEquals(testText, value);

            // Verify JMS properties are copied to Kafka headers
            Headers headers = sourceRecord.headers();
            assertThat(headers.lastWithName("source").value()).isEqualTo("system-a");
            assertThat(headers.lastWithName("retryCount").value()).isEqualTo("3");
            assertThat(headers.lastWithName("threshold").value()).isEqualTo("0.95");
            assertThat(headers.lastWithName("enabled").value()).isEqualTo("true");
        } finally {
            worker.stop();
        }
    }
}
