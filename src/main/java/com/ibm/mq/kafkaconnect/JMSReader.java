/**
 * Copyright 2017 IBM Corporation
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
import com.ibm.msg.client.wmq.WMQConstants;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.BytesMessage;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
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

    private boolean connected = false;                    // Whether connected to MQ
    private boolean inflight = false;                     // Whether messages in-flight in current transaction
    private boolean inperil = false;                      // Whether current transaction must be forced to roll back
    private AtomicBoolean closeNow = new AtomicBoolean(); // Whether close has been requested

    private static long RECEIVE_TIMEOUT = 30000l;

    /**
     * Constructor.
     *
     * @param queueManager       Queue manager name
     * @param connectionNameList Connection name list, comma-separated list of host(port) entries
     * @param channelName        Server-connection channel name
     * @param queueName          Queue name
     * @param userName           User name for authenticating to MQ, can be null
     * @param password           Password for authenticating to MQ, can be null
     * @param topic              Kafka topic name
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public JMSReader(String queueManager, String connectionNameList, String channelName, String queueName, String userName, String password, String topic) throws ConnectException {
        this.userName = userName;
        this.password = password;
        this.topic = topic;

        try {
            mqConnFactory = new MQConnectionFactory();
            mqConnFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqConnFactory.setQueueManager(queueManager);
            mqConnFactory.setConnectionNameList(connectionNameList);
            mqConnFactory.setChannel(channelName);

            queue = new MQQueue(queueName);
            messageBodyJms = false;
            queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
        }
        catch (JMSException | JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            throw new ConnectException(jmse);
        }
    }

    /**
     * Setter for message body as JMS.
     *
     * @param messageBodyJms     Whether to interpret the message body as a JMS message type
     */
    public void setMessageBodyJms(boolean messageBodyJms)
    {
        if (messageBodyJms != this.messageBodyJms) {
            this.messageBodyJms = messageBodyJms;
            try {
                if (!messageBodyJms) {
                    queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
                }
                else {
                    queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_JMS);
                }
            }
            catch (JMSException jmse) {
                ;
            }
        }
    }

    /**
     * Connects to MQ.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void connect() throws ConnectException, RetriableException {
        connectInternal();
        log.info("Connection to MQ established");
    }

    /**
     * Receives a message from MQ. Adds the message to the current transaction. Reconnects to MQ if required.
     *
     * @param wait                Whether to wait indefinitely for a message
     *
     * @return The SourceRecord representing the message
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public SourceRecord receive(boolean wait) throws ConnectException, RetriableException {
        connectInternal();

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
                sr = buildRecord(m);
            }
        }
        catch (JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
        }

        return sr;
    }

    /**
     * Commits the current transaction. If the current transaction contains a message that could not
     * be processed, the transaction is "in peril" and is rolled back instead to avoid data loss.
     *
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void commit() throws ConnectException, RetriableException {
        connectInternal();
        try {
            if (inflight) {
                inflight = false;

                if (inperil) {
                    inperil = false;
                    log.trace("Rolling back in-flight transaction");
                    jmsCtxt.rollback();
                    throw new RetriableException("Transaction rolled back");
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
     * @throws RetriableException Operation failed, but connector should continue to retry.
     * @throws ConnectException   Operation failed and connector should stop.
     */
    private void connectInternal() throws ConnectException, RetriableException {
        if (connected) {
            return;
        }

        if (closeNow.get()) {
            throw new ConnectException("Connection closing");
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
        }
        catch (JMSRuntimeException jmse) {
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
        }
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
        }
    }

    /**
     * Handles exceptions from MQ. Some JMS exceptions are treated as retriable meaning that the
     * connector can keep running and just trying again is likely to fix things.
     */
    private void handleException(Throwable exc) throws ConnectException, RetriableException {
        boolean isRetriable = false;
        boolean mustClose = true;
        int reason = -1;

        // Try to extract the MQ reason code to see if it's a retriable exception
        Throwable t = exc.getCause();
        while (t != null) {
            if (t instanceof MQException) {
                MQException mqe = (MQException)t;
                log.error("MQ error: CompCode {}, Reason {}", mqe.getCompCode(), mqe.getReason());
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

            // These reason codes indicates that the connect is still OK, but just retrying later
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
            throw new RetriableException(exc);
        }
        throw new ConnectException(exc);
    }

    /**
     * Builds a Kafka Connect SourceRecord from a JMS message.
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    private SourceRecord buildRecord(Message m) throws ConnectException {
        Schema valueSchema = null;
        Object value = null;

        // We've received a message in a transacted session so we must only permit the transaction
        // to commit once we've passed it on to Kafka. Temporarily mark the transaction as "in-peril"
        // so that any exception thrown will result in the transaction rolling back instead of committing.
        inperil = true;

        try {
            // Interpreting the body as a JMS message type, we can accept BytesMessages and TextMessage only.
            // We do not know the schema so do not specify one.
            if (messageBodyJms) {
                if (m instanceof BytesMessage) {
                    log.trace("Bytes message with no schema");
                    value = m.getBody(byte[].class);
                    inperil = false;
                }
                else if (m instanceof TextMessage) {
                    log.trace("Text message with no schema");
                    value = m.getBody(String.class);
                    inperil = false;
                }
                else {
                    log.error("Unsupported JMS message type {}", m.getClass());
                    throw new ConnectException("Unsupported JMS message type");
                }
            }
            else {
                // Not interpreting the body as a JMS message type, all messages come through as BytesMessage.
                // In this case, we specify the value schema as OPTIONAL_BYTES.
                if (m instanceof BytesMessage) {
                    log.trace("Bytes message with OPTIONAL_BYTES schema");
                    valueSchema = Schema.OPTIONAL_BYTES_SCHEMA;
                    value = m.getBody(byte[].class);
                    inperil = false;
                }
                else {
                    log.error("Unsupported JMS message type {}", m.getClass());
                    throw new ConnectException("Unsupported JMS message type");
                }
            }
        }
        catch (JMSException jmse) {
            log.debug("JMS exception {}", jmse);
            handleException(jmse);
        }

        return new SourceRecord(null, null, topic, valueSchema, value);
    }
}