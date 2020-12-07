/**
 * Copyright 2017, 2018, 2019 IBM Corporation
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MQSourceTask.class);

    // The maximum number of records returned per call to poll()
    private int batchSize = MQSourceConnector.CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT;
    private CountDownLatch batchCompleteSignal = null;              // Used to signal completion of a batch
    private AtomicInteger pollCycle = new AtomicInteger(1);         // Incremented each time poll() is called
    private int lastCommitPollCycle = 0;                            // The value of pollCycle the last time commit() was called
    private AtomicBoolean stopNow = new AtomicBoolean();            // Whether stop has been requested

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
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        for (final Entry<String, String> entry: props.entrySet()) {
            String value;
            if (entry.getKey().toLowerCase().contains("password")) {
                value = "[hidden]";
            } else {
                value = entry.getValue();
            }
            log.debug("Task props entry {} : {}", entry.getKey(), value);
        }

        String strBatchSize = props.get(MQSourceConnector.CONFIG_NAME_MQ_BATCH_SIZE);
        if (strBatchSize != null) {
            batchSize = Integer.parseInt(strBatchSize);
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

        // Resolve any in-flight transaction, committing unless there has been an error between
        // receiving the message from MQ and converting it
        if (batchCompleteSignal != null) {
            log.debug("Awaiting batch completion signal");
            batchCompleteSignal.await();

            log.debug("Committing records");
            reader.commit();
        }

        // Increment the counter for the number of times poll is called so we can ensure we don't get stuck waiting for
        // commitRecord callbacks to trigger the batch complete signal
        int currentPollCycle = pollCycle.incrementAndGet();
        log.debug("Starting poll cycle {}", currentPollCycle);

        try {
            if (!stopNow.get()) {
                log.info("Polling for records");
                SourceRecord src;
                do {
                    // For the first message in the batch, wait a while if no message
                    src = reader.receive(messageCount == 0);
                    if (src != null) {
                        msgs.add(src);
                        messageCount++;
                    }
                } while ((src != null) && (messageCount < batchSize) && !stopNow.get());
            }
            else {
                log.info("Stopping polling for records");
            }
        }
        finally {
        }

        synchronized(this) {
            if (messageCount > 0) {
                if (!stopNow.get()) {
                    batchCompleteSignal = new CountDownLatch(messageCount);
                }
                else {
                    // Discard this batch - we've rolled back when the connection to MQ was closed in stop()
                    log.debug("Discarding a batch of {} records as task is stopping", messageCount);
                    msgs.clear();
                    batchCompleteSignal = null;
                }
            }
            else {
                batchCompleteSignal = null;
            }
        }

        log.debug("Poll returning {} records", messageCount);

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

        // This callback is simply used to ensure that the mechanism to use commitRecord callbacks
        // to check that all messages in a batch are complete is not getting stuck. If this callback
        // is being called, it means that Kafka Connect believes that all outstanding messages have
        // been completed. That should mean that commitRecord has been called for all of them too.
        // However, if too few calls to commitRecord are received, the connector could wait indefinitely.
        // If this commit callback is called twice without the poll cycle increasing, trigger the
        // batch complete signal directly.
        int currentPollCycle = pollCycle.get();
        log.debug("Commit starting in poll cycle {}", currentPollCycle);

        if (lastCommitPollCycle == currentPollCycle)
        {
            synchronized (this) {
                if (batchCompleteSignal != null) {
                    log.debug("Bumping batch complete signal by {}", batchCompleteSignal.getCount());

                    // This means we're waiting for the signal in the poll() method and it's been
                    // waiting for at least two calls to this commit callback. It's stuck.
                    while (batchCompleteSignal.getCount() > 0) {
                        batchCompleteSignal.countDown();
                    }
                }
            }
        }
        else {
            lastCommitPollCycle = currentPollCycle;
        }

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

        stopNow.set(true);

        synchronized(this) {
            // Close the connection to MQ to clean up
            if (reader != null) {
                reader.close();
            }
        }

        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * <p>
     * Commit an individual {@link SourceRecord} when the callback from the producer client is received, or if a record is filtered by a transformation.
     * </p>
     * <p>
     * SourceTasks are not required to implement this functionality; Kafka Connect will record offsets
     * automatically. This hook is provided for systems that also need to store offsets internally
     * in their own system.
     * </p>
     *
     * @param record {@link SourceRecord} that was successfully sent via the producer.
     * @throws InterruptedException
     */
    @Override public void commitRecord(SourceRecord record) throws InterruptedException {
        log.trace("[{}] Entry {}.commitRecord, record={}", Thread.currentThread().getId(), this.getClass().getName(), record);

        synchronized (this) {
            batchCompleteSignal.countDown();
        }

        log.trace("[{}]  Exit {}.commitRecord", Thread.currentThread().getId(), this.getClass().getName());
    }
}