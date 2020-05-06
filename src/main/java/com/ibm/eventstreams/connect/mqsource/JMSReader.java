/**
 * Copyright 2017, 2020 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource;

import com.ibm.eventstreams.connect.mqsource.builders.RecordBuilder;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.jms.*;
import com.ibm.msg.client.wmq.WMQConstants;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

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

    private boolean connected = false;                              // Whether connected to MQ
    private boolean inflight = false;                               // Whether messages in-flight in current transaction
    private boolean inperil = false;                                // Whether current transaction must be forced to roll back
    private AtomicBoolean closeNow = new AtomicBoolean();           // Whether close has been requested
    private long reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN; // Delay between repeated reconnect attempts

    private static long RECEIVE_TIMEOUT = 30000l;
    private static long RECONNECT_DELAY_MILLIS_MIN = 64l;
    private static long RECONNECT_DELAY_MILLIS_MAX = 8192l;

    public JMSReader() {}

    /**
     * Configure this class.
     *
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void configure(Map<String, String> props) {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        String queueManager = props.get(MQSourceConnector.CONFIG_NAME_MQ_QUEUE_MANAGER);
        String connectionMode = props.get(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_MODE);
        String connectionNameList = props.get(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_NAME_LIST);
        String channelName = props.get(MQSourceConnector.CONFIG_NAME_MQ_CHANNEL_NAME);
        String queueName = props.get(MQSourceConnector.CONFIG_NAME_MQ_QUEUE);
        String userName = props.get(MQSourceConnector.CONFIG_NAME_MQ_USER_NAME);
        String password = props.get(MQSourceConnector.CONFIG_NAME_MQ_PASSWORD);
        String ccdtUrl = props.get(MQSourceConnector.CONFIG_NAME_MQ_CCDT_URL);
        String builderClass = props.get(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER);
        String mbj = props.get(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS);
        String mdr = props.get(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_MQMD_READ);
        String sslCipherSuite = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE);
        String sslPeerName = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_PEER_NAME);
        String sslKeystoreLocation = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION);
        String sslKeystorePassword = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD);
        String sslTruststoreLocation = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION);
        String sslTruststorePassword = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD);
        String useMQCSP = props.get(MQSourceConnector.CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP);
        String useIBMCipherMappings = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS);
        String topic = props.get(MQSourceConnector.CONFIG_NAME_TOPIC);

        if (useIBMCipherMappings != null) {
            System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", useIBMCipherMappings);
        }

        int transportType = WMQConstants.WMQ_CM_CLIENT;
        if (connectionMode != null) {
            if (connectionMode.equals(MQSourceConnector.CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT)) {
                transportType = WMQConstants.WMQ_CM_CLIENT;
            }
            else if (connectionMode.equals(MQSourceConnector.CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS)) {
                transportType = WMQConstants.WMQ_CM_BINDINGS;
            }
            else {
                log.error("Unsupported MQ connection mode {}", connectionMode);
                throw new ConnectException("Unsupported MQ connection mode");
            }
        }

        try {
            mqConnFactory = new MQConnectionFactory();
            mqConnFactory.setTransportType(transportType);
            mqConnFactory.setQueueManager(queueManager);
            mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
            if (useMQCSP != null) {
                mqConnFactory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, Boolean.parseBoolean(useMQCSP));
            }

            if (transportType == WMQConstants.WMQ_CM_CLIENT) {
                if (ccdtUrl != null) {
                    URL ccdtUrlObject;
                    try {
                        ccdtUrlObject = new URL(ccdtUrl);
                    }
                    catch (MalformedURLException e) {
                        log.error("MalformedURLException exception {}", e);
                        throw new ConnectException("CCDT file url invalid", e);
                    }
                    mqConnFactory.setCCDTURL(ccdtUrlObject);
                }
                else {
                    mqConnFactory.setConnectionNameList(connectionNameList);
                    mqConnFactory.setChannel(channelName);
                }

                if (sslCipherSuite != null) {
                    mqConnFactory.setSSLCipherSuite(sslCipherSuite);
                    if (sslPeerName != null)
                    {
                        mqConnFactory.setSSLPeerName(sslPeerName);
                    }
                }

                if (sslKeystoreLocation != null || sslTruststoreLocation != null) {
                    final SSLContext sslContext = buildSslContext(sslKeystoreLocation, sslKeystorePassword, sslTruststoreLocation, sslTruststorePassword);
                    mqConnFactory.setSSLSocketFactory(sslContext.getSocketFactory());
                }
            }

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

            if (mdr != null) {
                if (Boolean.parseBoolean(mdr)) {
                    queue.setBooleanProperty(WMQConstants.WMQ_MQMD_READ_ENABLED, true);
                }
            }

            this.topic = topic;
        }
        catch (JMSException | JMSRuntimeException jmse) {
            log.error("JMS exception {}", jmse);
            throw new ConnectException(jmse);
        }

        try {
            Class<? extends RecordBuilder> c = Class.forName(builderClass).asSubclass(RecordBuilder.class);
            builder = c.newInstance();
            builder.configure(props);
        }
        catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException | NullPointerException exc) {
            log.error("Could not instantiate message builder {}", builderClass);
            throw new ConnectException("Could not instantiate message builder", exc);
        }

        log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Connects to MQ.
     */
    public void connect() {
        log.trace("[{}] Entry {}.connect", Thread.currentThread().getId(), this.getClass().getName());

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
            log.error("JMS exception {}", jmse);
            handleException(jmse);
        }

        log.trace("[{}]  Exit {}.connect", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Receives a message from MQ. Adds the message to the current transaction. Reconnects to MQ if required.
     *
     * @param wait                Whether to wait indefinitely for a message
     *
     * @return The SourceRecord representing the message
     */
    public SourceRecord receive(boolean wait) {
        log.trace("[{}] Entry {}.receive", Thread.currentThread().getId(), this.getClass().getName());

        if (!connectInternal()) {
            log.trace("[{}]  Exit {}.receive, retval=null", Thread.currentThread().getId(), this.getClass().getName());
            return null;
        }

        Message m = null;
        SourceRecord sr = null;
        try {
            if (wait) {
                while ((m == null) && !closeNow.get())
                {
                    log.debug("Waiting {} ms for message", RECEIVE_TIMEOUT);
                    m = jmsCons.receive(RECEIVE_TIMEOUT);
                }

                if (m == null) {
                    log.debug("No message received");
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
        catch (JMSException | JMSRuntimeException exc) {
            log.error("JMS exception {}", exc);
            handleException(exc);
        }
        catch (ConnectException exc) {
            log.error("Connect exception {}", exc);
            throw exc;
        }

        log.trace("[{}]  Exit {}.receive, retval={}", Thread.currentThread().getId(), this.getClass().getName(), sr);
        return sr;
    }

    /**
     * Commits the current transaction. If the current transaction contains a message that could not
     * be processed, the transaction is "in peril" and is rolled back instead to avoid data loss.
     */
    public void commit() {
        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(), this.getClass().getName());

        if (!connectInternal()) {
            return;
        }

        try {
            if (inflight) {
                inflight = false;

                if (inperil) {
                    inperil = false;
                    log.debug("Rolling back in-flight transaction");
                    jmsCtxt.rollback();
                }
                else {
                    jmsCtxt.commit();
                }
            }
        }
        catch (JMSRuntimeException jmse) {
            log.error("JMS exception {}", jmse);
            handleException(jmse);
        }

        log.trace("[{}]  Exit {}.commit", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Closes the connection.
     */
    public void close() {
        log.trace("[{}] Entry {}.close", Thread.currentThread().getId(), this.getClass().getName());

        closeNow.set(true);
        closeInternal();

        log.trace("[{}]  Exit {}.close", Thread.currentThread().getId(), this.getClass().getName());
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
            log.debug("Closing connection now");
            return false;
        }

        log.trace("[{}] Entry {}.connectInternal", Thread.currentThread().getId(), this.getClass().getName());
        try {
            if (userName != null) {
                jmsCtxt = mqConnFactory.createContext(userName, password, JMSContext.SESSION_TRANSACTED);
            }
            else {
                jmsCtxt = mqConnFactory.createContext(JMSContext.SESSION_TRANSACTED);
            }

            jmsCons = jmsCtxt.createConsumer(queue);
            reconnectDelayMillis = RECONNECT_DELAY_MILLIS_MIN;
            connected = true;

            log.info("Connection to MQ established");
        }
        catch (JMSRuntimeException jmse) {
            // Delay slightly so that repeated reconnect loops don't run too fast
            try {
                Thread.sleep(reconnectDelayMillis);
            }
            catch (InterruptedException ie) {
                ;
            }

            if (reconnectDelayMillis < RECONNECT_DELAY_MILLIS_MAX)
            {
                reconnectDelayMillis = reconnectDelayMillis * 2;
            }

            log.error("JMS exception {}", jmse);
            handleException(jmse);
            log.trace("[{}]  Exit {}.connectInternal, retval=false", Thread.currentThread().getId(), this.getClass().getName());
            return false;
        }

        log.trace("[{}]  Exit {}.connectInternal, retval=true", Thread.currentThread().getId(), this.getClass().getName());
        return true;
    }

    /**
     * Internal method to close the connection.
     */
    private void closeInternal() {
        log.trace("[{}] Entry {}.closeInternal", Thread.currentThread().getId(), this.getClass().getName());

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

        log.trace("[{}]  Exit {}.closeInternal", Thread.currentThread().getId(), this.getClass().getName());
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
            else if (t instanceof JMSException) {
                JMSException jmse = (JMSException)t;
                log.error("JMS exception: error code {}", jmse.getErrorCode());
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

            // These reason codes indicate that the connection is still OK, but just retrying later
            // will probably recover - possibly with administrative action on the queue manager
            case MQConstants.MQRC_GET_INHIBITED:
                isRetriable = true;
                mustClose = false;
                break;
        }

        if (mustClose) {
            // Delay so that repeated reconnect loops don't run too fast
            try {
                Thread.sleep(RECONNECT_DELAY_MILLIS_MAX);
            }
            catch (InterruptedException ie) {
                ;
            }
            closeInternal();
        }

        if (isRetriable) {
            return new RetriableException(exc);
        }

        return new ConnectException(exc);
    }

    private SSLContext buildSslContext(String sslKeystoreLocation, String sslKeystorePassword, String sslTruststoreLocation, String sslTruststorePassword) {
        log.trace("[{}] Entry {}.buildSslContext", Thread.currentThread().getId(), this.getClass().getName());

        try {
            KeyManager[] keyManagers = null;
            TrustManager[] trustManagers = null;

            if (sslKeystoreLocation != null) {
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(loadKeyStore(sslKeystoreLocation, sslKeystorePassword), sslKeystorePassword.toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            if (sslTruststoreLocation != null) {
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(loadKeyStore(sslTruststoreLocation, sslTruststorePassword));
                trustManagers = tmf.getTrustManagers();
            }

            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagers, new SecureRandom());

            log.trace("[{}]  Exit {}.buildSslContext, retval={}", Thread.currentThread().getId(), this.getClass().getName(), sslContext);
            return sslContext;
        } catch (GeneralSecurityException e) {
            throw new ConnectException("Error creating SSLContext", e);
        }
    }

    private KeyStore loadKeyStore(String location, String password) throws GeneralSecurityException {
        log.trace("[{}] Entry {}.loadKeyStore", Thread.currentThread().getId(), this.getClass().getName());

        try (final InputStream ksStr = new FileInputStream(location)) {
            final KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksStr, password.toCharArray());

            log.trace("[{}]  Exit {}.loadKeyStore, retval={}", Thread.currentThread().getId(), this.getClass().getName(), ks);
            return ks;
        } catch (IOException e) {
            throw new ConnectException("Error reading keystore " + location, e);
        }
    }
}