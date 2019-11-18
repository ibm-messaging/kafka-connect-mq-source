/**
 * Copyright 2017, 2018, 2019 IBM Corporation
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

import static java.nio.charset.StandardCharsets.*;

import java.util.HashMap;
import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds Kafka Connect SourceRecords from messages. It parses the bytes of the payload of JMS
 * BytesMessage and TextMessage as JSON and creates a SourceRecord with a null schema.
 */
public class JsonRecordBuilder extends BaseRecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(JsonRecordBuilder.class);

    private JsonConverter converter;

    public JsonRecordBuilder() {
        log.info("Building records using com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        converter = new JsonConverter();

        // We just want the payload, not the schema in the output message
        HashMap<String, String> m = new HashMap<>();
        m.put("schemas.enable", "false");

        // Convert the value, not the key (isKey == false)
        converter.configure(m, false);
    }

    /**
     * Gets the value schema to use for the Kafka Connect SourceRecord.
     *
     * @param context            the JMS context to use for building messages
     * @param topic              the Kafka topic
     * @param messageBodyJms     whether to interpret MQ messages as JMS messages
     * @param message            the message
     *
     * @return the Kafka Connect SourceRecord's value
     *
     * @throws JMSException      Message could not be converted
     */
    @Override public SchemaAndValue getValue(JMSContext context, String topic, boolean messageBodyJms, Message message) throws JMSException {
        byte[] payload;

        if (message instanceof BytesMessage) {
            payload = message.getBody(byte[].class);
        }
        else if (message instanceof TextMessage) {
            String s = message.getBody(String.class);
            payload = s.getBytes(UTF_8);
        }
        else {
            log.error("Unsupported JMS message type {}", message.getClass());
            throw new ConnectException("Unsupported JMS message type");
        }

        return converter.toConnectData(topic, payload);
    }
}