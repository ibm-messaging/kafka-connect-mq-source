/**
 * Copyright 2023, 2024 IBM Corporation
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

import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getJmsConnectionFactory;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.ibm.msg.client.jms.JmsConnectionFactory;

public class MQConnectionRollbackHelper {
    
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageConsumer consumer;

    public List<Message> readNoCommit(final String queueName, final int batchSize) throws JMSException {

        final JmsConnectionFactory cf = getJmsConnectionFactory();

        connection = cf.createConnection();
        session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

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
        
        return messages;
    }

    public void rollback() throws JMSException {
        session.rollback();
    }
}
