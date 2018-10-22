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
package com.ibm.eventstreams.connect.mqsource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MQSourceTask.class);

    private static int BATCH_SIZE = 100;
    private static int MAX_UNCOMMITTED_MSGS = 10000;
    private static int MAX_UNCOMMITTED_MSGS_DELAY_MS = 500;

    private JMSReader reader;
    private AtomicInteger uncommittedMessages = new AtomicInteger(0);

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
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        for (final Entry<String, String> entry: props.entrySet()) {
            log.debug("Task props entry {} : {}", entry.getKey(), entry.getValue());
        }

        // Construct a reader to interface with MQ
        reader = new JMSReader();
        reader.configure(props);

        // Make a connection as an initial test of the configuration
        reader.connect();

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Poll this SourceTask for new records. This method should block if no data is currently
     * available.
     *
     * @return a list of source records
     */
    @Override public List<SourceRecord> poll() throws InterruptedException {
        log.trace("[{}] Entry {}.poll", Thread.currentThread().getId(), this.getClass().getName());

        final List<SourceRecord> msgs = new ArrayList<>();
        int messageCount = 0;
        int uncommittedMessagesInt = this.uncommittedMessages.get();

        if (uncommittedMessagesInt < MAX_UNCOMMITTED_MSGS) {
            log.info("Polling for records");

            SourceRecord src;
            do {
                // For the first message in the batch, wait a while if no message
                src = reader.receive(messageCount == 0);
                if (src != null) {
                    msgs.add(src);
                    messageCount++;
                    uncommittedMessagesInt = this.uncommittedMessages.incrementAndGet();
                }
            } while ((src != null) && (messageCount < BATCH_SIZE) && (uncommittedMessagesInt < MAX_UNCOMMITTED_MSGS));

            log.debug("Poll returning {} records", messageCount);
        }
        else {
            log.info("Uncommitted message limit reached");
            Thread.sleep(MAX_UNCOMMITTED_MSGS_DELAY_MS);
        }

        log.trace("[{}]  Exit {}.poll, retval={}", Thread.currentThread().getId(), this.getClass().getName(), messageCount);
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
        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(), this.getClass().getName());

        log.debug("Committing records");
        reader.commit();
        this.uncommittedMessages.set(0);

        log.trace("[{}]  Exit {}.commit", Thread.currentThread().getId(), this.getClass().getName());
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
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());

        if (reader != null) {
            reader.close();
        }

        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }
}