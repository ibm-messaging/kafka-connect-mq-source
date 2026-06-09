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

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

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
            final List<String> jmsPropertyKeys = Collections.list(propertyNames);

            jmsPropertyKeys.forEach(key -> {
                try {
                    final Object prop;
            
                    if (key.equals(JmsConstants.JMS_IBM_MQMD_MSGID)) {
                        prop = message.getJMSMessageID();
                    } else if (key.equals(JmsConstants.JMS_IBM_MQMD_CORRELID)) {
                        prop = message.getJMSCorrelationID();
                    } else {
                        prop = message.getObjectProperty(key);
                    }

                    log.debug("Adding JMS property {} with value {}", key, prop);
                    addHeaderWithType(connectHeaders, key, prop);
                } catch (final JMSException e) {
                    // Not failing the message processing if JMS properties cannot be read for some
                    // reason.
                    log.warn("Could not copy property {} from the JMS message due to exception {}", key, e);
                }
            });
        } catch (final JMSException e) {
            // Not failing the message processing if JMS properties cannot be read for some
            // reason.
            log.warn("JMS exception {}", e);
        }

        return connectHeaders;
    }

    /**
     * Adds a header to ConnectHeaders
     * - Only MQMD properties like GroupId/AccountingToken can be byte[]
     *   when mq.message.mqmd.read=true
     * - For any other types, convert to String
     *
     * @param headers The ConnectHeaders to add to
     * @param key The header key
     * @param value The header value
     */
    private void addHeaderWithType(final ConnectHeaders headers, final String key, final Object value) {

        if (value == null) {
            headers.addString(key, null);
            return;
        }

        if (value instanceof byte[]) {
            // Only MQMD properties like GroupId/AccountingToken can be byte[]
            // JMS spec does not allow custom properties to be byte[] - only MQMD properties (when mq.message.mqmd.read=true)
            log.debug("Converting property '{}' from byte[]: {}", key, (byte[]) value);
            headers.addBytes(key, (byte[]) value);
        } else {
            // For any other types, convert to String
            log.debug("Converting property '{}' of type '{}' to String", key, value.getClass().getName());
            headers.addString(key, value.toString());
        }
    }
}
