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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.TextMessage;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT;
import com.ibm.eventstreams.connect.mqsource.JMSWorker;

public class JsonRecordBuilderIT extends AbstractJMSContextIT {

    private final String topic = "MY.TOPIC";
    private final boolean isJMS = true;

    private final String messageContents =
        "{ " +
            "\"hello\" : \"world\", " +
            "\"test\" : 123, " +
            "\"list\" : [ \"one\", \"two\", \"three\" ] " +
        "}";

    @SuppressWarnings("unchecked")
    private void verifyJsonMap(final Map<?, ?> value) {
        assertEquals(3, value.keySet().size());
        assertEquals("world", value.get("hello"));
        assertEquals(123L, value.get("test"));
        final String[] expected = {"one", "two", "three"};
        assertArrayEquals(expected, ((List<String>) value.get("list")).toArray());
    }

    @Test
    public void buildFromJmsTextMessage() throws Exception {
        // create MQ message
        final TextMessage message = getJmsContext().createTextMessage(messageContents);

        // use the builder to convert it to a Kafka record
        final JsonRecordBuilder builder = new JsonRecordBuilder();
        final SourceRecord record = builder.toSourceRecord(getJmsContext(), topic, isJMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertNull(record.valueSchema());
        verifyJsonMap((Map<?, ?>) record.value());
    }

    @Test
    public void buildFromJmsBytesMessage() throws Exception {
        // create MQ message
        final BytesMessage message = getJmsContext().createBytesMessage();
        message.writeBytes(messageContents.getBytes());
        message.reset();

        // use the builder to convert it to a Kafka record
        final JsonRecordBuilder builder = new JsonRecordBuilder();
        final SourceRecord record = builder.toSourceRecord(getJmsContext(), topic, isJMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertNull(record.valueSchema());
        verifyJsonMap((Map<?, ?>) record.value());
    }

    @Test
    public void buildFromJmsMapMessage() throws Exception {
        final String messageContents = "This is the message contents";

        // create MQ message
        final MapMessage message = getJmsContext().createMapMessage();
        message.setString("example", messageContents);

        // use the builder to convert it to a Kafka record
        final JsonRecordBuilder builder = new JsonRecordBuilder();
        final RecordBuilderException exc = assertThrows(RecordBuilderException.class, () -> {
            builder.toSourceRecord(getJmsContext(), topic, isJMS, message);
        });

        // verify the exception
        assertEquals("Unsupported JMS message type", exc.getMessage());
    }

    @Test
    public void buildFromJmsTestJsonError() throws Exception {
        // create MQ message
        final TextMessage message = getJmsContext().createTextMessage("Not a valid json string");

        // use the builder to convert it to a Kafka record
        final JsonRecordBuilder builder = new JsonRecordBuilder();
        final DataException exec = assertThrows(DataException.class, () -> builder.toSourceRecord(getJmsContext(), topic, isJMS, message));
        assertEquals("Converting byte[] to Kafka Connect data failed due to serialization error: ", exec.getMessage());
    }

    @Test
    public void buildFromJmsTestErrorTolerance() throws Exception {
        // create MQ message
        final TextMessage message = getJmsContext().createTextMessage("Not a valid json string");

        // use the builder to convert it to a Kafka record
        final JsonRecordBuilder builder = new JsonRecordBuilder();
        final Map<String, String> config = AbstractJMSContextIT.getDefaultConnectorProperties();
        config.put("errors.tolerance", "all");
        config.put("mq.message.body.jms", "true");
        config.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        builder.configure(config);
        final SourceRecord record = builder.toSourceRecord(getJmsContext(), topic, isJMS, message);
        assertNull(record);
    }

    @Test
    public void buildFromJmsTestErrorToleranceNone() throws Exception {
        // create MQ message
        final TextMessage message = getJmsContext().createTextMessage("Not a valid json string");

        // use the builder to convert it to a Kafka record
        final JsonRecordBuilder builder = new JsonRecordBuilder();
        final HashMap<String, String> config = new HashMap<String, String>();
        config.put("errors.tolerance", "none");
        config.put("mq.message.body.jms", "true");
        config.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");

        builder.configure(config);
        assertThrows(DataException.class, () -> {
            builder.toSourceRecord(getJmsContext(), topic, isJMS, message);
        });
    }


    @Test
    public void testToSourceRecord_JsonRecordBuilder_JsonMessage() throws Exception {
        // Test: JsonRecordBuilder with JSON message and JsonConverter
        // Expected: JSON output with no schema
        Map<String, String> connectorProps = getDefaultConnectorProperties();
        connectorProps.put("mq.message.body.jms", "true"); // Not used by JsonRecordBuilder
        connectorProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        connectorProps.put("mq.jms.properties.copy.to.kafka.headers", "true");

        JMSWorker worker = new JMSWorker();
        worker.configure(getPropertiesConfig(connectorProps));
        worker.connect();

        try {
            String jsonText = "{ \"id\": 123, \"name\": \"test\", \"active\": true }";
            TextMessage textMessage = getJmsContext().createTextMessage(jsonText);
            textMessage.setStringProperty("source", "system-a");
            textMessage.setIntProperty("retryCount", 3);
            textMessage.setDoubleProperty("threshold", 0.95);
            textMessage.setBooleanProperty("enabled", true);

            Map<String, Long> sourceOffset = new HashMap<>();
            sourceOffset.put("sequence-id", 4L);

            Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put("source", "myqmgr/myq");

            SourceRecord sourceRecord = worker.toSourceRecord(textMessage, true, sourceOffset, sourcePartition);

            assertThat(sourceRecord).isNotNull();
            assertThat(sourceRecord.value()).isInstanceOf(Map.class);
            assertNull(sourceRecord.valueSchema()); // JSON with no schema
            
            // Verify JSON data
            @SuppressWarnings("unchecked")
            Map<String, Object> value = (Map<String, Object>) sourceRecord.value();
            assertEquals(123L, value.get("id"));
            assertEquals("test", value.get("name"));
            assertEquals(true, value.get("active"));

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
