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

import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds Kafka Connect SourceRecords from messages. This is the default implementation.
 * <ul>
 * <li>For MQ messages, the SourceRecord has an OPTIONAL_BYTES schema and a byte[] value.
 * <li>For JMS BytesMessage, the SourceRecord has no schema and a byte[] value.
 * <li>For JMS TextMessage, the SourceRecord has no schema and a string value.
 * </ul>
 */
public class DefaultRecordBuilder extends BaseRecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(DefaultRecordBuilder.class);

    public DefaultRecordBuilder() {
        log.info("Building records using com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
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
        Schema valueSchema = null;
        Object value = null;

        // Interpreting the body as a JMS message type, we can accept BytesMessage and TextMessage only.
        // We do not know the schema so do not specify one.
        if (messageBodyJms) {
            if (message instanceof BytesMessage) {
                log.debug("Bytes message with no schema");
                value = message.getBody(byte[].class);
            }
            else if (message instanceof TextMessage) {
                log.debug("Text message with no schema");
                value = message.getBody(String.class);
            }
            else {
                log.error("Unsupported JMS message type {}", message.getClass());
                throw new ConnectException("Unsupported JMS message type");
            }
        }
        else {
            // Not interpreting the body as a JMS message type, all messages come through as BytesMessage.
            // In this case, we specify the value schema as OPTIONAL_BYTES.
            log.debug("Bytes message with OPTIONAL_BYTES schema");
            valueSchema = Schema.OPTIONAL_BYTES_SCHEMA;
            value = message.getBody(byte[].class);
        }

        return new SchemaAndValue(valueSchema, value);
    }
}