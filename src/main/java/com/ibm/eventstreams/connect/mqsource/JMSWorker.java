/**
 * Copyright 2017, 2020, 2023, 2024 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsource;

import com.ibm.eventstreams.connect.mqsource.builders.RecordBuilder;
import com.ibm.eventstreams.connect.mqsource.builders.RecordBuilderFactory;
import com.ibm.eventstreams.connect.mqsource.builders.RecordBuilderException;
import com.ibm.eventstreams.connect.mqsource.util.QueueConfig;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.mq.jms.MQQueue;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.QueueBrowser;
import javax.jms.TextMessage;
import javax.net.ssl.SSLContext;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reads messages from MQ using JMS. Uses a transacted session, adding messages
 * to the current
 * transaction until told to commit. Automatically reconnects as needed.
 */
public class JMSWorker {
    private static final Logger log = LoggerFactory.getLogger(JMSWorker.class);

    // Configs
    private String userName;
    private Password password;
    private String topic;

    // JMS factory and context
    private MQConnectionFactory mqConnFactory;
    private JMSContext jmsCtxt;

    final private HashMap<String, JMSConsumer> jmsConsumers = new HashMap<>();
    private RecordBuilder recordBuilder;

    private boolean connected = false; // Whether connected to MQ
    private AtomicBoolean closeNow; // Whether close has been requested
    private long reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN; // Delay between repeated reconnect attempts

    private static final long RECEIVE_TIMEOUT = 2000L;
    private static final long RECONNECT_DELAY_MILLIS_MIN = 64L;
    private static final long RECONNECT_DELAY_MILLIS_MAX = 8192L;

    /**
     * Configure this class.
     *
     * @param config initial configuration
     * @throws JMSWorkerConnectionException
     */
    public void configure(final AbstractConfig config) {

        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(),
                config);

        System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings",
                config.getBoolean(MQSourceConnector.CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS).toString());

        final int transportType =
                config.getString(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_MODE)
                        .equals(MQSourceConnector.CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT) ?
                        WMQConstants.WMQ_CM_CLIENT :
                        WMQConstants.WMQ_CM_BINDINGS;

        try {
            mqConnFactory = new MQConnectionFactory();
            mqConnFactory.setTransportType(transportType);
            mqConnFactory.setQueueManager(config.getString(MQSourceConnector.CONFIG_NAME_MQ_QUEUE_MANAGER));
            mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP,
                    config.getBoolean(MQSourceConnector.CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP));

            if (transportType == WMQConstants.WMQ_CM_CLIENT) {
                final String ccdtUrl = config.getString(MQSourceConnector.CONFIG_NAME_MQ_CCDT_URL);

                if (ccdtUrl != null) {
                    mqConnFactory.setCCDTURL(new URL(ccdtUrl));
                } else {
                    mqConnFactory.setConnectionNameList(config.getString(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_NAME_LIST));
                    mqConnFactory.setChannel(config.getString(MQSourceConnector.CONFIG_NAME_MQ_CHANNEL_NAME));
                }

                mqConnFactory.setSSLCipherSuite(config.getString(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE));
                mqConnFactory.setSSLPeerName(config.getString(MQSourceConnector.CONFIG_NAME_MQ_SSL_PEER_NAME));


                final String sslKeystoreLocation = config.getString(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION);
                final Password sslKeystorePassword = config.getPassword(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD);
                final String sslTruststoreLocation = config.getString(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION);
                final Password sslTruststorePassword = config.getPassword(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD);
                if (sslKeystoreLocation != null || sslTruststoreLocation != null) {
                    final SSLContext sslContext = new SSLContextBuilder().buildSslContext(sslKeystoreLocation, sslKeystorePassword,
                            sslTruststoreLocation, sslTruststorePassword);
                    mqConnFactory.setSSLSocketFactory(sslContext.getSocketFactory());
                }
            }

            userName = config.getString(MQSourceConnector.CONFIG_NAME_MQ_USER_NAME);
            password = config.getPassword(MQSourceConnector.CONFIG_NAME_MQ_PASSWORD);
            topic = config.getString(MQSourceConnector.CONFIG_NAME_TOPIC);
        } catch (JMSException | JMSRuntimeException jmse) {
            log.error("JMS exception {}", jmse);
            throw new JMSWorkerConnectionException("JMS connection failed", jmse);
        } catch (final MalformedURLException e) {
            log.error("MalformedURLException exception {}", e);
            throw new ConnectException("CCDT file url invalid", e);
        }
        closeNow = new AtomicBoolean();
        closeNow.set(false);
        this.recordBuilder = RecordBuilderFactory.getRecordBuilder(config.originalsStrings());

        log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
    }


    /**
     * Used for tests.
     */
    protected void setRecordBuilder(final RecordBuilder recordBuilder) {
        this.recordBuilder = recordBuilder;
    }

    protected JMSContext getContext() { // used to enable testing 
        if (jmsCtxt == null) maybeReconnect();
        return jmsCtxt;
    }

    /**
     * Connects to MQ.
     */
    public void connect() {
        log.trace("[{}] Entry {}.connect", Thread.currentThread().getId(), this.getClass().getName());
        if (userName != null) {
            this.jmsCtxt = mqConnFactory.createContext(userName, password.value(), JMSContext.SESSION_TRANSACTED);
        } else {
            this.jmsCtxt = mqConnFactory.createContext(JMSContext.SESSION_TRANSACTED);
        }

        connected = true;

        log.info("Connection to MQ established");
        log.trace("[{}]  Exit {}.connect", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Receives a message from MQ. Adds the message to the current transaction.
     * Reconnects to MQ if required.
     *
     * @param queueName   The name of the queue to get messages from
     * @param queueConfig Any particular queue configuration that should be applied
     * @param wait        Whether to wait indefinitely for a message
     * @return The Message retrieved from MQ
     */
    public Message receive(final String queueName, final QueueConfig queueConfig, final boolean wait) throws JMSRuntimeException, JMSException {
        log.trace("[{}] Entry {}.receive", Thread.currentThread().getId(), this.getClass().getName());

        if (!maybeReconnect()) {
            log.trace("[{}]  Exit {}.receive, retval=null", Thread.currentThread().getId(), this.getClass().getName());
            return null;
        }

        final JMSConsumer internalConsumer;
        if (jmsConsumers.containsKey(queueName)) {
            internalConsumer = jmsConsumers.get(queueName);
        } else {
            MQQueue queue = new MQQueue(queueName);
            queue = queueConfig.applyToQueue(queue);
            internalConsumer = jmsCtxt.createConsumer(queue);
            jmsConsumers.put(queueName, internalConsumer);
        }

        Message message = null;
        if (wait) {
            log.debug("Waiting {} ms for message", RECEIVE_TIMEOUT);

            message = internalConsumer.receive(RECEIVE_TIMEOUT);

            if (message == null) {
                log.debug("No message received");
            }
        } else {
            message = internalConsumer.receiveNoWait();
        }

        log.trace("[{}]  Exit {}.receive, retval={}", Thread.currentThread().getId(), this.getClass().getName(), message);

        return message;
    }

    public Optional<Message> browse(final String queueName) throws JMSRuntimeException, JMSException {
        final QueueBrowser internalBrowser = jmsCtxt.createBrowser(new MQQueue(queueName));
        final Message message;
        final Enumeration<?> e = internalBrowser.getEnumeration();
        if (e.hasMoreElements()) {
            message = (Message) e.nextElement(); // two messages (true) or one message (false)
        } else {
            message = null; // no message
        }
        internalBrowser.close();
        return Optional.ofNullable(message);
    }

    /**
     * Browses the queue and returns true if there are at least two messages and false if there is zero or one message.
     * The method does not read or return any of the messages.
     *
     * @param queueName String. Name of the queue to be browsed
     * @return boolean
     * @throws JMSException
     */
    public boolean queueHoldsMoreThanOneMessage(final String queueName) throws JMSException {
        final QueueBrowser internalBrowser;
        final boolean moreThanOneMessageOnQueue;
        internalBrowser = jmsCtxt.createBrowser(new MQQueue(queueName));
        final Enumeration<?> e = internalBrowser.getEnumeration();
        if (e.hasMoreElements()) {
            e.nextElement(); //get first
            moreThanOneMessageOnQueue = e.hasMoreElements(); // two messages (true) or one message (false)
        } else {
            moreThanOneMessageOnQueue = false; // no message
        }
        internalBrowser.close();
        return moreThanOneMessageOnQueue;
    }

    /**
     * Writes a message to a queue
     * @throws JMSException
     */
    public void putTextMessage(final String payload, final String queueName) throws JMSRuntimeException, JMSException {

        if (!maybeReconnect()) {
            log.trace("[{}]  Exit {}.receive, retval=null", Thread.currentThread().getId(), this.getClass().getName());
            return;
        }

        final TextMessage message = jmsCtxt.createTextMessage(payload);
        final JMSProducer localProducer = jmsCtxt.createProducer();
        localProducer.send(new MQQueue(queueName), message);
    }

    /**
     * Returns messages got from the MQ queue. Called if the builder has failed to
     * transform the
     * messages and return them to Connect for producing to Kafka.
     */
    public void attemptRollback() {

        if (!maybeReconnect()) {
            log.warn("[{}]  Exit {}.attemptRollback, retval=null, connection failed", Thread.currentThread().getId(), this.getClass().getName());
            return;
        }

        log.trace("[{}] Entry {}.attemptRollback", Thread.currentThread().getId(), this.getClass().getName());
        try {
            jmsCtxt.rollback();
        } catch (final JMSRuntimeException jmsExc) {
            log.error("rollback failed {0}", jmsExc);
        }
        log.trace("[{}]  Exit {}.attemptRollback", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Commits the current transaction.
     */
    public void commit() {
        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(), this.getClass().getName());

        if (!maybeReconnect()) {
            return;
        }

        jmsCtxt.commit();

        log.trace("[{}]  Exit {}.commit", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Closes the connection.
     */
    public void stop() {
        log.trace("[{}] Entry {}.close", Thread.currentThread().getId(), this.getClass().getName());

        closeNow.set(true);
        close();

        log.trace("[{}]  Exit {}.close", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Internal method to reconnect to MQ.
     *
     * @return true if connection can be used, false otherwise
     */
    private boolean maybeReconnect() throws JMSRuntimeException {
        if (connected) {
            return true;
        }

        if (closeNow.get()) {
            log.debug("Closing connection now");
            return false;
        }

        log.trace("[{}] Entry {}.maybeReconnect", Thread.currentThread().getId(), this.getClass().getName());
        try {
            connect();
            reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN;
            log.info("Connection to MQ established");
        } catch (final JMSRuntimeException jmse) {
            // Delay slightly so that repeated reconnect loops don't run too fast
            try {
                Thread.sleep(reconnectDelayMillis);
            } catch (final InterruptedException ie) {
            }

            if (reconnectDelayMillis < RECONNECT_DELAY_MILLIS_MAX) {
                reconnectDelayMillis = reconnectDelayMillis * 2;
            }

            log.error("JMS exception {}", jmse);
            log.trace("[{}]  Exit {}.maybeReconnect, retval=JMSRuntimeException", Thread.currentThread().getId(),
                    this.getClass().getName());
            throw jmse;
        }

        log.trace("[{}]  Exit {}.maybeReconnect, retval=true", Thread.currentThread().getId(),
                this.getClass().getName());
        return true;
    }

    /**
     * Internal method to close the connection.
     */
    public void close() {
        log.trace("[{}] Entry {}.close", Thread.currentThread().getId(), this.getClass().getName());

        try {
            connected = false;

            jmsConsumers.clear();

            if (jmsCtxt != null) {
                jmsCtxt.close();
            }
        } catch (final JMSRuntimeException jmse) {
            log.error("", jmse);
        } finally {
            jmsCtxt = null;
            log.debug("Connection to MQ closed");
        }

        log.trace("[{}]  Exit {}.close", Thread.currentThread().getId(), this.getClass().getName());
    }

    public SourceRecord toSourceRecord(final Message message, final boolean messageBodyJms, final Map<String, Long> sourceOffset, final Map<String, String> sourcePartition) {
        try {
            return recordBuilder.toSourceRecord(jmsCtxt, topic, messageBodyJms, message, sourceOffset, sourcePartition);
        } catch (final JMSException e) {
            throw new RecordBuilderException(e);
        }
    }
}