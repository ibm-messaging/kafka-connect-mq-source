/**
 * Copyright 2019, 2024, 2026 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource.processor;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.msg.client.jms.JmsConstants;

import javax.jms.JMSException;
import javax.jms.Message;

import java.util.Enumeration;

/**
 * Single responsibility class to copy JMS properties to Kafka headers.
 * Converts all JMS properties to String except byte[]
 */
public class JmsToKafkaHeaderConverter {
    private static final Logger log = LoggerFactory.getLogger(JmsToKafkaHeaderConverter.class);
    /**
     * Copies the JMS properties to Kafka headers.
     *
     * @param message JMS message.
     *
     * @return Kafka connect headers.
     */
    public ConnectHeaders convertJmsPropertiesToKafkaHeaders(final Message message) {
        final ConnectHeaders connectHeaders = new ConnectHeaders();

        try {
            @SuppressWarnings("unchecked")
            final Enumeration<String> propertyNames = (Enumeration<String>) message.getPropertyNames();
            while (propertyNames.hasMoreElements()) {
                copyPropertyToHeader(message, connectHeaders, propertyNames.nextElement());
            }
        } catch (final JMSException e) {
            // Allow message processing to continue but log failure
            log.warn("Failed to read JMS property names from message", e);
        }

        return connectHeaders;
    }

    private void copyPropertyToHeader(final Message message, final ConnectHeaders connectHeaders, final String key) {
        try {
            final Object prop;

            if (key.equals(JmsConstants.JMS_IBM_MQMD_MSGID)) {
                // special case - instead of using getObjectProperty("JMS_IBM_MQMD_MsgId")
                //  to return the byte array, we use the JMS method that returns it as
                //  an "ID:"-prefixed hex string
                prop = message.getJMSMessageID();
            } else if (key.equals(JmsConstants.JMS_IBM_MQMD_CORRELID)) {
                // special case - instead of using getObjectProperty("JMS_IBM_MQMD_CorrelId")
                //  to return the byte array, we use the JMS method that returns it as
                //  an "ID:"-prefixed hex string
                prop = message.getJMSCorrelationID();
            } else {
                prop = message.getObjectProperty(key);
            }

            log.debug("Adding JMS property {} with value {}", key, prop);

            if (prop instanceof byte[]) {
                connectHeaders.addBytes(key, (byte[]) prop);
            } else {
                // this yields `null` if prop is null, otherwise its toString()
                connectHeaders.addString(key, prop != null ? prop.toString() : null);
            }
        } catch (final JMSException e) {
            // Allow message processing to continue but log failure
            log.warn("Could not copy property {} from the JMS message", key, e);
        }
    }
}
