/**
 * Copyright 2023 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource.utils;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.CHANNEL_NAME;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.QMGR_NAME;
import static com.ibm.eventstreams.connect.mqsource.AbstractJMSContextIT.TCP_MQ_HOST_PORT;

public class MQTestUtil {

    private static final Logger log = LoggerFactory.getLogger(MQTestUtil.class);

    public static final String mqContainer = "icr.io/ibm-messaging/mq:latest";

    /**
     * Puts all messages to the specified MQ queue. Used in tests to
     * give the Connector something to get.
     */
    public static void putAllMessagesToQueue(final String queueName, final List<Message> messages) throws JMSException {
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageProducer producer = null;

        final JmsConnectionFactory cf = getJmsConnectionFactory();

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        destination = session.createQueue(queueName);
        producer = session.createProducer(destination);

        connection.start();

        for (final Message message : messages) {
            message.setJMSDestination(destination);
            producer.send(message);
        }

        connection.close();
    }

    @NotNull
    public static JmsConnectionFactory getJmsConnectionFactory() throws JMSException {
        final JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);

        final JmsConnectionFactory cf = ff.createConnectionFactory();
        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, "localhost");
        cf.setIntProperty(WMQConstants.WMQ_PORT, TCP_MQ_HOST_PORT);
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, CHANNEL_NAME);
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, QMGR_NAME);
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, false);
        return cf;
    }

    public static void removeAllMessagesFromQueue(String queueName) throws JMSException {
        log.info("Starting to remove messages...");
        getAllMessagesFromQueue(queueName);
        log.info("Done removing messages...");
    }

    /**
     * Gets all messages from the specified MQ queue. Used in tests to
     * verify what is left on the test queue
     */
    public static List<Message> getAllMessagesFromQueue(final String queueName) throws JMSException {
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer consumer = null;

        final JmsConnectionFactory cf = getJmsConnectionFactory();

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);

        connection.start();

        final List<Message> messages = new ArrayList<>();
        Message message;
        do {
            message = consumer.receiveNoWait();
            if (message != null) {
                messages.add(message);
            }
        }
        while (message != null);

        connection.close();

        return messages;
    }

    public static List<Message> getSingleBatchOfMessagesFromQueue(final String queueName, final int batchSize) throws JMSException {
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer consumer = null;

        final JmsConnectionFactory cf = getJmsConnectionFactory();

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);

        connection.start();

        final List<Message> messages = new ArrayList<>();
        int messageCount = 0;
        Message message;
        do {
            message = consumer.receiveNoWait();
            if (message != null) {
                messages.add(message);
                messageCount++;
            }
        }
        while (message != null && messageCount < batchSize);
        
        session.rollback();

        connection.close();
        
        return messages;
    }

    public static List<String> getIDsOfMessagesCurrentlyOnQueue(String queueName, int batchSize) throws JMSException {
        List<Message> messages = browseMessagesInBatch(queueName, batchSize);

        return messages.stream().map(m -> {
            try {
                return m.getJMSMessageID();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    public static List<String> getIDsOfMessagesCurrentlyOnQueue(String queueName) throws JMSException {
        List<Message> messages = browseAllMessagesFromQueue(queueName);

        return messages.stream().map(m -> {
            try {
                return m.getJMSMessageID();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
    }

    public static int getMessageCount(final String queueName) throws JMSException {
        return browseAllMessagesFromQueue(queueName).size();
    }

    public static List<Message> browseAllMessagesFromQueue(final String queueName) throws JMSException {
        Connection connection = null;
        Session session = null;

        final JmsConnectionFactory cf = getJmsConnectionFactory();

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(queueName);
        QueueBrowser browser = session.createBrowser(queue);

        connection.start();

        final List<Message> messages = new ArrayList<>();

        Enumeration e = browser.getEnumeration();
        while (e.hasMoreElements()) {
            messages.add((Message) e.nextElement());
        }

        connection.close();

        return messages;
    }

    public static List<Message> browseMessagesInBatch(final String queueName, int batchSize) throws JMSException {
        Connection connection = null;
        Session session = null;

        final JmsConnectionFactory cf = getJmsConnectionFactory();

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Queue queue = session.createQueue(queueName);
        QueueBrowser browser = session.createBrowser(queue);

        connection.start();

        final List<Message> messages = new ArrayList<>();

        Enumeration e = browser.getEnumeration();
        while (messages.size() < batchSize) {
            messages.add((TextMessage) e.nextElement());
        }

        connection.close();

        return messages;
    }

}
