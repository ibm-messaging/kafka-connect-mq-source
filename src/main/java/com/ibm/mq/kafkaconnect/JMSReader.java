/**
 * Copyright 2017, 2018 IBM Corporation
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
package com.ibm.mq.kafkaconnect;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.jms.*;
import com.ibm.mq.kafkaconnect.builders.RecordBuilder;
import com.ibm.msg.client.wmq.WMQConstants;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reads messages from MQ using JMS. Uses a transacted session, adding messages to the current
 * transaction until told to commit. Automatically reconnects as needed.
 */
public class JMSReader {
    private static final Logger log = LoggerFactory.getLogger(JMSReader.class);

    // Configs
    private String userName;
    private String password;
    private String topic;
    private boolean messageBodyJms;

    // JMS factory and context
    private MQConnectionFactory mqConnFactory;
    private JMSContext jmsCtxt;
    private JMSConsumer jmsCons;
    private MQQueue queue;

    private RecordBuilder builder;
    
    private boolean connected = false;                    // Whether connected to MQ
    private boolean inflight = false;                     // Whether messages in-flight in current transaction
    private boolean inperil = false;                      // Whether current transaction must be forced to roll back
    private AtomicBoolean closeNow = new AtomicBoolean(); // Whether close has been requested

    private static long RECEIVE_TIMEOUT = 30000l;

    public JMSReader() {}

    /**
     * Configure this class.
     * 
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void configure(Map<String, String> props) {
        String queueManager = props.get(MQSourceConnector.CONFIG_NAME_MQ_QUEUE_MANAGER);
        String connectionNameList = props.get(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_NAME_LIST);
        String channelName = props.get(MQSourceConnector.CONFIG_NAME_MQ_CHANNEL_NAME);
        String queueName = props.get(MQSourceConnector.CONFIG_NAME_MQ_QUEUE);
        String userName = props.get(MQSourceConnector.CONFIG_NAME_MQ_USER_NAME);
        String password = props.get(MQSourceConnector.CONFIG_NAME_MQ_PASSWORD);
        String builderClass = props.get(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER);
        String mbj = props.get(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS);
        String sslCipherSuite = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE);
        String sslPeerName = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_PEER_NAME);
        String topic = props.get(MQSourceConnector.CONFIG_NAME_TOPIC);

        try {
            mqConnFactory = new MQConnectionFactory();
            mqConnFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqConnFactory.setQueueManager(queueManager);
            mqConnFactory.setConnectionNameList(connectionNameList);
            mqConnFactory.setChannel(channelName);
            queue = new MQQueue(queueName);
            
            this.userName = userName;
            this.password = password;
    
            this.messageBodyJms = false;
            queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
            if (mbj != null) {
                if (Boolean.parseBoolean(mbj)) {
                    this.messageBodyJms = true;
                    queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_JMS);
                }
            }

            if (sslCipherSuite != null) {
                mqConnFactory.setSSLCipherSuite(sslCipherSuite);
                if (sslPeerName != null)
                {
                    mqConnFactory.setSSLPeerName(sslPeerName);
                }
            }

            this.topic = topic;
        }
        catch (JMSException | JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            throw new ConnectException(jmse);
        }

        try {
            Class<? extends RecordBuilder> c = Class.forName(builderClass).asSubclass(RecordBuilder.class);
            builder = c.newInstance();
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NullPointerException exc) {
            log.debug("Could not instantiate message builder {}", builderClass);
            throw new ConnectException("Could not instantiate message builder", exc);
        }
    }

    /**
     * Connects to MQ.
     */
    public void connect() {
        try {
            if (userName != null) {
                jmsCtxt = mqConnFactory.createContext(userName, password, JMSContext.SESSION_TRANSACTED);
            }
            else {
                jmsCtxt = mqConnFactory.createContext(JMSContext.SESSION_TRANSACTED);
            }            

            jmsCons = jmsCtxt.createConsumer(queue);
            connected = true;
        
            log.info("Connection to MQ established");
        }
        catch (JMSRuntimeException jmse) {
            log.info("Connection to MQ could not be established");
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
        }
    }

    /**
     * Receives a message from MQ. Adds the message to the current transaction. Reconnects to MQ if required.
     *
     * @param wait                Whether to wait indefinitely for a message
     *
     * @return The SourceRecord representing the message
     */
    public SourceRecord receive(boolean wait) {
        if (!connectInternal()) {
            return null;
        }

        Message m = null;
        SourceRecord sr = null;
        try {
            if (wait) {
                while ((m == null) && !closeNow.get())
                {
                    log.trace("Waiting {} ms for message", RECEIVE_TIMEOUT);
                    m = jmsCons.receive(RECEIVE_TIMEOUT);
                }

                if (m == null) {
                    log.trace("No message received");
                }
            }
            else {
                m = jmsCons.receiveNoWait();
            }

            if (m != null) {
                inflight = true;

                // We've received a message in a transacted session so we must only permit the transaction
                // to commit once we've passed it on to Kafka. Temporarily mark the transaction as "in-peril"
                // so that any exception thrown will result in the transaction rolling back instead of committing.
                inperil = true;
                
                sr = builder.toSourceRecord(jmsCtxt, topic, messageBodyJms, m);
                inperil = false;
            }
        }
        catch (JMSException | JMSRuntimeException | ConnectException exc) {
            log.debug("JMS exception {}", exc);
            handleException(exc);
        }

        return sr;
    }

    /**
     * Commits the current transaction. If the current transaction contains a message that could not
     * be processed, the transaction is "in peril" and is rolled back instead to avoid data loss.
     */
    public void commit() {
        if (!connectInternal()) {
            return;
        }

        try {
            if (inflight) {
                inflight = false;

                if (inperil) {
                    inperil = false;
                    log.trace("Rolling back in-flight transaction");
                    jmsCtxt.rollback();
                }
                else {
                    jmsCtxt.commit();
                }
            }
        }
        catch (JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
        }
    }

    /**
     * Closes the connection.
     */
    public void close() {
        try {
            JMSContext ctxt = jmsCtxt;
            closeNow.set(true);
            ctxt.close();
        }
        catch (JMSRuntimeException jmse) {
            ;
        }
    }

    /**
     * Internal method to connect to MQ.
     *
     * @return true if connection can be used, false otherwise
     */
    private boolean connectInternal() {
        if (connected) {
            return true;
        }

        if (closeNow.get()) {
            return false;
        }

        try {
            if (userName != null) {
                jmsCtxt = mqConnFactory.createContext(userName, password, JMSContext.SESSION_TRANSACTED);
            }
            else {
                jmsCtxt = mqConnFactory.createContext(JMSContext.SESSION_TRANSACTED);
            }            

            jmsCons = jmsCtxt.createConsumer(queue);
            connected = true;
        
            log.info("Connection to MQ established");
        }
        catch (JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
            return false;
        }

        return true;
    }

    /**
     * Internal method to close the connection.
     */
    private void closeInternal() {
        try {
            inflight = false;
            inperil = false;
            connected = false;

            if (jmsCtxt != null) {
                jmsCtxt.close();
            }
        }
        catch (JMSRuntimeException jmse) {
            ;
        }
        finally
        {
            jmsCtxt = null;
            log.debug("Connection to MQ closed");
        }
    }

    /**
     * Handles exceptions from MQ. Some JMS exceptions are treated as retriable meaning that the
     * connector can keep running and just trying again is likely to fix things.
     */
    private ConnectException handleException(Throwable exc) {
        boolean isRetriable = false;
        boolean mustClose = true;
        int reason = -1;

        // Try to extract the MQ reason code to see if it's a retriable exception
        Throwable t = exc.getCause();
        while (t != null) {
            if (t instanceof MQException) {
                MQException mqe = (MQException)t;
                log.error("MQ error: CompCode {}, Reason {} {}", mqe.getCompCode(), mqe.getReason(),
                          MQConstants.lookupReasonCode(mqe.getReason()));
                reason = mqe.getReason();
                break;
            }
            t = t.getCause();
        }

        switch (reason)
        {
            // These reason codes indicate that the connection needs to be closed, but just retrying later
            // will probably recover
            case MQConstants.MQRC_BACKED_OUT:
            case MQConstants.MQRC_CHANNEL_NOT_AVAILABLE:
            case MQConstants.MQRC_CONNECTION_BROKEN:
            case MQConstants.MQRC_HOST_NOT_AVAILABLE:
            case MQConstants.MQRC_NOT_AUTHORIZED:
            case MQConstants.MQRC_Q_MGR_NOT_AVAILABLE:
            case MQConstants.MQRC_Q_MGR_QUIESCING:
            case MQConstants.MQRC_Q_MGR_STOPPING:
            case MQConstants.MQRC_UNEXPECTED_ERROR:
                isRetriable = true;
                break;

            // These reason codes indicate that the connect is still OK, but just retrying later
            // will probably recover - possibly with administrative action on the queue manager
            case MQConstants.MQRC_GET_INHIBITED:
                isRetriable = true;
                mustClose = false;
                break;
        }

        if (mustClose) {
            closeInternal();
        }

        if (isRetriable) {
            return new RetriableException(exc);
        }

        return new ConnectException(exc);
    }
}