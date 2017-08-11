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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MQSourceTask.class);

    // Configs
    private String queueManager;
    private String connectionNameList;
    private String channelName;
    private String queueName;
    private String userName;
    private String password;
    private String sslCipherSuite;
    private String topic;

    private static int BATCH_SIZE = 50;

    private JMSReader reader;

    public MQSourceTask() {
    }

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return MQSourceConnector.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override public void start(Map<String, String> props) {
        for (final Entry<String, String> entry: props.entrySet()) {
            log.trace("Task props entry {} : {}", entry.getKey(), entry.getValue());
        }

        queueManager = props.get(MQSourceConnector.CONFIG_NAME_MQ_QUEUE_MANAGER);
        connectionNameList = props.get(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_NAME_LIST);
        channelName = props.get(MQSourceConnector.CONFIG_NAME_MQ_CHANNEL_NAME);
        queueName = props.get(MQSourceConnector.CONFIG_NAME_MQ_QUEUE);
        userName = props.get(MQSourceConnector.CONFIG_NAME_MQ_USER_NAME);
        password = props.get(MQSourceConnector.CONFIG_NAME_MQ_PASSWORD);
        sslCipherSuite = props.get(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE);
        topic = props.get(MQSourceConnector.CONFIG_NAME_TOPIC);

        // Construct a reader to interface with MQ
        reader = new JMSReader(queueManager, connectionNameList, channelName, queueName, userName, password, topic);

        if (sslCipherSuite != null) {
            reader.setSSLConfiguration(sslCipherSuite);
        }

        String mbj = props.get(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS);
        if (mbj != null) {
            reader.setMessageBodyJms(Boolean.parseBoolean(mbj));
        }

        // Make a connection as an initial test of the configuration
        reader.connect();
    }

    /**
     * Poll this SourceTask for new records. This method should block if no data is currently
     * available.
     *
     * @return a list of source records
     */
    @Override public List<SourceRecord> poll() throws InterruptedException {
        final List<SourceRecord> msgs = new ArrayList<>();
        int messageCount = 0;

        log.info("Polling for records");
        SourceRecord src;
        do {
            // For the first message in the batch, wait indefinitely
            src = reader.receive(messageCount == 0 ? true : false);
            if (src != null) {
                msgs.add(src);
                messageCount++;
            }
        } while ((src != null) && (messageCount < BATCH_SIZE));

        log.trace("Poll returning {} records", messageCount);
        return msgs;
    }


    /**
     * <p>
     * Commit the offsets, up to the offsets that have been returned by {@link #poll()}. This
     * method should block until the commit is complete.
     * </p>
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     * </p>
     */
    public void commit() throws InterruptedException {
        log.trace("Committing records");
        reader.commit();
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a different thread than {@link #poll()} and
     * {@link #commit()}.
     *
     * For example, if a task uses a {@link java.nio.channels.Selector} to receive data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and invoke
     * {@link java.nio.channels.Selector#wakeup() wakeup()} to interrupt any ongoing requests.
     */
    @Override public void stop() {
        if (reader != null) {
            reader.close();
        }
    }
}