/**
 * Copyright 2022 IBM Corporation
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.TextMessage;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT;

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
        final ConnectException exc = assertThrows(ConnectException.class, () -> {
            builder.toSourceRecord(getJmsContext(), topic, isJMS, message);
        });

        // verify the exception
        assertEquals("Unsupported JMS message type", exc.getMessage());
    }
}
