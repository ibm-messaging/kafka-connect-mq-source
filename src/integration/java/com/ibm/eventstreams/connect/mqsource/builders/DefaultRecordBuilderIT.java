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
import static org.junit.Assert.assertTrue;

import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT;

public class DefaultRecordBuilderIT extends AbstractJMSContextIT {

    private static final String TOPIC = "MY.TOPIC";


    @Test
    public void buildFromJmsMapMessage() throws Exception {
        final String MESSAGE_CONTENTS = "This is the message contents";
        final boolean IS_JMS = true;

        // create MQ message
        MapMessage message = getJmsContext().createMapMessage();
        message.setString("example", MESSAGE_CONTENTS);

        // use the builder to convert it to a Kafka record
        DefaultRecordBuilder builder = new DefaultRecordBuilder();
        ConnectException exc = assertThrows(ConnectException .class, () -> {
            builder.toSourceRecord(getJmsContext(), TOPIC, IS_JMS, message);
        });

        // verify the exception
        assertEquals("Unsupported JMS message type", exc.getMessage());
    }


    @Test
    public void buildFromJmsTextMessage() throws Exception {
        final String MESSAGE_CONTENTS = "This is the JMS message contents";
        final boolean IS_JMS = true;

        // create MQ message
        TextMessage message = getJmsContext().createTextMessage(MESSAGE_CONTENTS);

        // use the builder to convert it to a Kafka record
        DefaultRecordBuilder builder = new DefaultRecordBuilder();
        SourceRecord record = builder.toSourceRecord(getJmsContext(), TOPIC, IS_JMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertEquals(MESSAGE_CONTENTS, record.value());
        assertNull(record.valueSchema());
    }


    @Test
    public void buildFromTextMessage() throws Exception {
        final String MESSAGE_CONTENTS = "This is the message contents";
        final boolean IS_JMS = false;

        // create MQ message
        TextMessage message = getJmsContext().createTextMessage(MESSAGE_CONTENTS);

        // use the builder to convert it to a Kafka record
        DefaultRecordBuilder builder = new DefaultRecordBuilder();
        MessageFormatException exc = assertThrows(MessageFormatException.class, () -> {
            builder.toSourceRecord(getJmsContext(), TOPIC, IS_JMS, message);
        });

        // verify the exception
        assertEquals("JMSCC5002", exc.getErrorCode());
        assertTrue(exc.getMessage().contains("The message of type jms_text can not have its body assigned to"));
    }


    @Test
    public void buildFromJmsBytesMessage() throws Exception {
        final String MESSAGE_ORIGIN = "This is the data used for message contents";
        final byte[] MESSAGE_CONTENTS = MESSAGE_ORIGIN.getBytes();
        final boolean IS_JMS = true;

        // create MQ message
        BytesMessage message = getJmsContext().createBytesMessage();
        message.writeBytes(MESSAGE_CONTENTS);
        message.reset();

        // use the builder to convert it to a Kafka record
        DefaultRecordBuilder builder = new DefaultRecordBuilder();
        SourceRecord record = builder.toSourceRecord(getJmsContext(), TOPIC, IS_JMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertArrayEquals(MESSAGE_CONTENTS, (byte[])record.value());
        assertNull(record.valueSchema());
    }


    @Test
    public void buildFromBytesMessage() throws Exception {
        final String MESSAGE_ORIGIN = "This is the data used for message contents";
        final byte[] MESSAGE_CONTENTS = MESSAGE_ORIGIN.getBytes();
        final boolean IS_JMS = false;

        // create MQ message
        BytesMessage message = getJmsContext().createBytesMessage();
        message.writeBytes(MESSAGE_CONTENTS);
        message.reset();

        // use the builder to convert it to a Kafka record
        DefaultRecordBuilder builder = new DefaultRecordBuilder();
        SourceRecord record = builder.toSourceRecord(getJmsContext(), TOPIC, IS_JMS, message);

        // verify the Kafka record
        assertNull(record.key());
        assertArrayEquals(MESSAGE_CONTENTS, (byte[])record.value());
        assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, record.valueSchema());
    }
}
