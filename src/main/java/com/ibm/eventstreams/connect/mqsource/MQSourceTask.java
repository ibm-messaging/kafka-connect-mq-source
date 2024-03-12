/**
 * Copyright 2017, 2018, 2019, 2023 IBM Corporation
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

import com.ibm.eventstreams.connect.mqsource.builders.RecordBuilderException;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceState;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateClient;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateException;
import com.ibm.eventstreams.connect.mqsource.util.LogMessages;
import com.ibm.eventstreams.connect.mqsource.util.ExceptionProcessor;
import com.ibm.eventstreams.connect.mqsource.util.QueueConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.ibm.eventstreams.connect.mqsource.MQSourceConnector.CONFIG_NAME_MQ_BATCH_SIZE;
import static com.ibm.eventstreams.connect.mqsource.MQSourceConnector.CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE;
import static com.ibm.eventstreams.connect.mqsource.MQSourceConnector.CONFIG_NAME_MQ_QUEUE;
import static com.ibm.eventstreams.connect.mqsource.MQSourceConnector.CONFIG_NAME_MQ_QUEUE_MANAGER;
import static com.ibm.eventstreams.connect.mqsource.MQSourceConnector.CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT;

import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskStartUpAction.REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE;
import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskStartUpAction.NORMAL_OPERATION;
import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskStartUpAction.REDELIVER_UNSENT_BATCH;
import static com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceState.LastKnownState.DELIVERED;
import static com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceState.LastKnownState.IN_FLIGHT;

public class MQSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MQSourceTask.class);

    // The maximum number of records returned per call to poll()
    private int batchSize = CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT;
    private CountDownLatch batchCompleteSignal = null; // Used to signal completion of a batch
    private AtomicLong pollCycle = new AtomicLong(1); // Incremented each time poll() is called
    private AtomicLong sequenceStateId = new AtomicLong(0);
    private List<String> msgIds = new ArrayList<String>();
    private long lastCommitPollCycle = 0; // The value of pollCycle the last time commit() was called
    private AtomicBoolean stopNow = new AtomicBoolean(); // Whether stop has been requested
    private boolean isExactlyOnceMode;
    private String sourceQueue;
    private QueueConfig sourceQueueConfig;
    private JMSWorker reader;
    private JMSWorker dedicated;
    private SequenceStateClient sequenceStateClient;
    private Map<String, String> sourceQueuePartition;

    private int startActionPollLimit = 300; // This is a 5 minute time out on the initial start procedure
    private AtomicInteger startActionPollCount = new AtomicInteger(0);

    private final static String OFFSET_IDENTIFIER = "sequence-id";
    private final static String SOURCE_PARTITION_IDENTIFIER = "source";

    public MQSourceTaskStartUpAction startUpAction;

    protected CountDownLatch getBatchCompleteSignal() {
        return batchCompleteSignal;
    }


    /**
     * Get the version of this task. Usually this should be the same as the
     * corresponding {@link //Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return MQSourceConnector.version;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time
     * setup of the task.
     *
     * @param props initial configuration
     */

    @Override
    public void start(final Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);
        final JMSWorker reader = new JMSWorker();
        JMSWorker dedicated = null;
        SequenceStateClient client = null;
        if (MQSourceConnector.configSupportsExactlyOnce(props)) {
            dedicated = new JMSWorker();
            client = new SequenceStateClient(props.get(CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE), reader, dedicated);
        }
        start(props, reader, dedicated, client);
        log.trace("[{}] Exit {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);
    }

    protected void start(final Map<String, String> props, final JMSWorker reader, final JMSWorker dedicated, final SequenceStateClient sequenceStateClient) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);
        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, props, true);

        this.reader = reader;
        this.dedicated = dedicated;
        this.sequenceStateClient = sequenceStateClient;
        this.isExactlyOnceMode = MQSourceConnector.configSupportsExactlyOnce(props);
        this.sourceQueueConfig = new QueueConfig(props);
        this.sourceQueuePartition = Collections.singletonMap(
                SOURCE_PARTITION_IDENTIFIER,
                props.get(CONFIG_NAME_MQ_QUEUE_MANAGER) + "/" + props.get(CONFIG_NAME_MQ_QUEUE)
        );

        startUpAction = NORMAL_OPERATION;

        batchSize = config.getInt(CONFIG_NAME_MQ_BATCH_SIZE);
        try {
            reader.configure(config);
            reader.connect();

            if (isExactlyOnceMode) {
                log.debug(" Deciding startup behaviour from state provided in the state queue and Kafka offsets for exactly once processing.");
                dedicated.configure(config);
                dedicated.connect();

                sequenceStateClient.validateStateQueue();
                final Optional<SequenceState> mqSequenceState = sequenceStateClient.browse();
                final Optional<Long> kafkaSequenceState = sequenceStateClient.getSequenceFromKafkaOffset(context, OFFSET_IDENTIFIER, sourceQueuePartition);

                startUpAction = determineStartupAction(mqSequenceState, kafkaSequenceState);
                sequenceStateId.set(mqSequenceState.map(SequenceState::getSequenceId).orElseGet(() -> kafkaSequenceState.orElse(SequenceState.DEFAULT_SEQUENCE_ID))); // get sequenceId from MQ state or Kafka or Default
                mqSequenceState.ifPresent(sequenceState -> msgIds.addAll(sequenceState.getMessageIds())); // if there is an MQ state take the msgIds from there;

                if (startUpAction == REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE && mqSequenceState.get().isInFlight()) {
                    sequenceStateClient.replaceState(new SequenceState(sequenceStateId.get(), msgIds, DELIVERED)); // change deliveryState from InFlight to Delivered
                }
            }

        } catch (JMSRuntimeException | JMSException | JMSWorkerConnectionException e) {
            log.error("MQ Connection Exception: ", e);
            closeAllWorkers();
            throw new ConnectException(e);
        } catch (final SequenceStateException e) {
            log.error(LogMessages.UNEXPECTED_MESSAGE_ON_STATE_QUEUE, e);
            closeAllWorkers();
            throw new ConnectException(e);
        } catch (final ConnectException e) {
            log.error("Unexpected connect exception: ", e);
            closeAllWorkers();
            throw e;
        } catch (final RuntimeException e) {
            log.error(LogMessages.UNEXPECTED_EXCEPTION, e);
            closeAllWorkers();
            throw e;
        }

        sourceQueue = props.get(CONFIG_NAME_MQ_QUEUE);

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    private MQSourceTaskStartUpAction determineStartupAction(final Optional<SequenceState> mqSequenceState, final Optional<Long> kafkaSequenceState) {

        if (mqSequenceState.isPresent()) {
            if (mqSequenceState.get().isDelivered()) {
                log.debug(" There are messages on MQ that have been delivered to the topic already. Removing delivered messages from the source queue.");
                return REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE;
            } else if (mqSequenceState.get().isInFlight() && mqSequenceMatchesKafkaSequence(mqSequenceState.get(), kafkaSequenceState)) {
                log.debug(" There are messages on MQ that have been delivered to the topic already. Removing delivered messages from the source queue.");
                return REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE;
            } else if (mqSequenceState.get().isInFlight() && !kafkaSequenceState.isPresent() ||
                    mqSequenceState.get().isInFlight() && !mqSequenceMatchesKafkaSequence(mqSequenceState.get(), kafkaSequenceState)) {
                log.debug(" There are messages on MQ that need to be redelivered to Kafka as the previous attempt failed.");
                return REDELIVER_UNSENT_BATCH;
            }
        }
        log.debug(" The state queue is empty. Proceeding to normal operation.");
        return NORMAL_OPERATION;
    }

    private static boolean mqSequenceMatchesKafkaSequence(final SequenceState mqSequenceState, final Optional<Long> kafkaSequenceState) {
        return mqSequenceState.getSequenceId() == kafkaSequenceState.orElse(-1L);
    }

    /**
     * Poll this SourceTask for new records. This method should block if no data is
     * currently
     * available.
     *
     * @return a list of source records
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.trace("[{}] Entry {}.poll", Thread.currentThread().getId(), this.getClass().getName());
        try {
            return internalPoll();
        } catch (JMSRuntimeException | JMSException e) {
            log.error("JMS Exception: ", e);
            maybeCloseAllWorkers(e);
            throw ExceptionProcessor.handleException(e);
        } catch (final RecordBuilderException e) {
            batchCompleteSignal = null;
            maybeCloseAllWorkers(e);
            throw new ConnectException(e);
        } catch (final ConnectException e) {
            log.error("Unexpected connect exception: ", e);
            maybeCloseAllWorkers(e);
            throw e;
        } catch (final RuntimeException e) {
            log.error(LogMessages.UNEXPECTED_EXCEPTION, e);
            maybeCloseAllWorkers(e);
            throw e;
        }
    }

    private List<SourceRecord> internalPoll() throws InterruptedException, JMSRuntimeException, JMSException {
        final List<Message> messageList;

        // Resolve any in-flight transaction, committing unless there has been an error
        // between receiving the message from MQ and converting it
        if (batchCompleteSignal != null) waitForKafkaThenCommitMQ();

        // Increment the counter for the number of times poll is called so we can ensure
        // we don't get stuck waiting for
        // commitRecord callbacks to trigger the batch complete signal
        log.debug("Starting poll cycle {}", pollCycle.incrementAndGet());

        log.info(" {}.internalPoll: acting on startup action", this.getClass().getName());
        switch (startUpAction) {
            case REMOVE_DELIVERED_MESSAGES_FROM_SOURCE_QUEUE:
                if (isFirstMsgOnSourceQueueARequiredMsg(msgIds)) {
                    removeDeliveredMessagesFromSourceQueue(msgIds);
                    startUpAction = NORMAL_OPERATION;
                    log.debug(" Delivered message have been removed from the source queue and will not be forwarded to Kafka.");
                } else { // The messages could still be locked in a tx on mq.
                    log.debug(" Delivered message have not been rolled back to the source queue.");
                    maybeFailWithTimeoutIfNotWaitAndIncrement(msgIds);
                }
                return Collections.emptyList();

            case REDELIVER_UNSENT_BATCH:
                if (isFirstMsgOnSourceQueueARequiredMsg(msgIds)) {
                    messageList = new ArrayList<>(pollSourceQueue(msgIds.size()));
                    log.debug(" The task has retrieved undelivered messages from the source queue.");
                } else { // The messages could still be locked in a tx on mq.
                    log.debug(" Delivered message have not been rolled back to the source queue.");
                    maybeFailWithTimeoutIfNotWaitAndIncrement(msgIds);
                    return Collections.emptyList();
                }
                break;

            case NORMAL_OPERATION:
                messageList = new ArrayList<>(pollSourceQueue(batchSize));
                startActionPollCount.set(0);
                if (messageList.size() == 0) {
                    // There were no messages
                    log.debug(" There were no messages.");
                    initOrResetBatchCompleteSignal(false, messageList);
                    return Collections.emptyList();
                }
                if (isExactlyOnceMode) {
                    sequenceStateId.incrementAndGet();
                }
                break;

            default:
                log.warn(" {}.internalPoll: Entered default case. Start has failed to set the startup action for the connector.", this.getClass().getName());
                return Collections.emptyList();
        }

        // if we're here then there were messages on the queue
        initOrResetBatchCompleteSignal(messageList.size() > 0, messageList);

        final HashMap<String, Long> sourceOffset;
        if (isExactlyOnceMode) {
            log.debug(" Adding the sequence id as the offset within the source records.");
            sourceOffset = new HashMap<>();
            sourceOffset.put(OFFSET_IDENTIFIER, sequenceStateId.get());
        } else {
            sourceOffset = null;
        }

        final ArrayList<String> msgIds = new ArrayList<>();
        final List<SourceRecord> sourceRecordList = messageList.stream()
                .peek(saveMessageID(msgIds))
                .map(message -> reader.toSourceRecord(message, sourceQueueConfig.isMqMessageBodyJms(), sourceOffset, sourceQueuePartition))
                .collect(Collectors.toList());

        // In RE-DELIVER we already have a state on the queue
        if (isExactlyOnceMode && startUpAction == NORMAL_OPERATION) {
            sequenceStateClient.write(
                    new SequenceState(
                            sequenceStateId.get(),
                            msgIds,
                            IN_FLIGHT)
            );
        }

        log.debug("Poll returning {} records", messageList.size());
        log.trace("[{}]  Exit {}.poll, retval={}", Thread.currentThread().getId(), this.getClass().getName(), messageList.size());

        return sourceRecordList;
    }

    private static Consumer<Message> saveMessageID(final ArrayList<String> msgIds) {
        return message -> {
            try {
                msgIds.add(message.getJMSMessageID());
            } catch (final JMSException e) {
                throw new RecordBuilderException(e);
            }
        };
    }

    private void initOrResetBatchCompleteSignal(final boolean predicate, final List<Message> messageList) {
        synchronized (this) {
            if (predicate) {
                if (!stopNow.get()) {
                    batchCompleteSignal = new CountDownLatch(messageList.size());
                } else {
                    // Discard this batch - we've rolled back when the connection to MQ was closed
                    // in stop()
                    log.debug("Discarding a batch of {} records as task is stopping", messageList.size());
                    messageList.clear();
                    batchCompleteSignal = null;
                }
            } else {
                batchCompleteSignal = null;
            }
        }
    }

    private List<Message> pollSourceQueue(final int numberOfMessagesToBePolled) throws JMSException {
        final List<Message> localList = new ArrayList<>();

        if (!stopNow.get()) {
            log.debug("Polling for records");
            Message message;
            do {
                message = reader.receive(sourceQueue, sourceQueueConfig, localList.size() == 0);
                if (message != null) {
                    localList.add(message);
                }
            } while (message != null && localList.size() < numberOfMessagesToBePolled && !stopNow.get());
        } else {
            log.info("Stopping polling for records");
        }
        return localList;
    }

    private boolean isFirstMsgOnSourceQueueARequiredMsg(final List<String> msgIds) throws JMSException {
        final Message message = reader.browse(sourceQueue).get();
        return msgIds.contains(message.getJMSMessageID());
    }

    private void waitForKafkaThenCommitMQ() throws InterruptedException, JMSRuntimeException, JMSException {
        log.debug("Awaiting batch completion signal");
        batchCompleteSignal.await();

        if (isExactlyOnceMode) {
            sequenceStateClient.retrieveStateInSharedTx();
        }

        log.debug("Committing records");
        reader.commit();
        startUpAction = NORMAL_OPERATION;
    }

    /**
     *
     * Commit the offsets, up to the offsets that have been returned by
     * {@link #poll()}. This
     * method should block until the commit is complete.
     *
     *
     * SourceTasks are not required to implement this functionality; Kafka Connect
     * will record offsets
     * automatically. This hook is provided for systems that also need to store
     * offsets internally
     * in their own system.
     *
     */
    public void commit() throws InterruptedException {
        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(), this.getClass().getName());

        // This callback is simply used to ensure that the mechanism to use commitRecord
        // callbacks
        // to check that all messages in a batch are complete is not getting stuck. If
        // this callback
        // is being called, it means that Kafka Connect believes that all outstanding
        // messages have
        // been completed. That should mean that commitRecord has been called for all of
        // them too.
        // However, if too few calls to commitRecord are received, the connector could
        // wait indefinitely.
        // If this commit callback is called twice without the poll cycle increasing,
        // trigger the
        // batch complete signal directly.
        final long currentPollCycle = pollCycle.get();
        log.debug("Commit starting in poll cycle {}", currentPollCycle);

        if (lastCommitPollCycle == currentPollCycle) {
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
        } else {
            lastCommitPollCycle = currentPollCycle;
        }

        log.trace("[{}]  Exit {}.commit", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Signal this SourceTask to stop. In SourceTasks, this method only needs to
     * signal to the task that it should stop
     * trying to poll for new data and interrupt any outstanding poll() requests. It
     * is not required that the task has
     * fully stopped. Note that this method necessarily may be invoked from a
     * different thread than {@link #poll()} and
     * {@link #commit()}.
     *
     * For example, if a task uses a {@link java.nio.channels.Selector} to receive
     * data over the network, this method
     * could set a flag that will force {@link #poll()} to exit immediately and
     * invoke
     * {@link java.nio.channels.Selector#wakeup() wakeup()} to interrupt any ongoing
     * requests.
     */
    @Override
    public void stop() {
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());

        stopNow.set(true);

        synchronized (this) {
            // Close the connections to MQ to clean up
            if (reader != null) {
                reader.stop();
            }
            if (dedicated != null) {
                dedicated.stop();
            }
        }

        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     *
     * Commit an individual {@link SourceRecord} when the callback from the producer
     * client is received, or if a record is filtered by a transformation.
     *
     *
     * SourceTasks are not required to implement this functionality; Kafka Connect
     * will record offsets
     * automatically. This hook is provided for systems that also need to store
     * offsets internally
     * in their own system.
     *
     *
     * @param record {@link SourceRecord} that was successfully sent via the
     *               producer.
     * @throws InterruptedException
     */
    @Override
    public void commitRecord(final SourceRecord record) throws InterruptedException {
        log.trace("[{}] Entry {}.commitRecord, record={}", Thread.currentThread().getId(), this.getClass().getName(),
                record);

        synchronized (this) {
            batchCompleteSignal.countDown();
        }

        log.trace("[{}]  Exit {}.commitRecord", Thread.currentThread().getId(), this.getClass().getName());
    }

    protected void removeDeliveredMessagesFromSourceQueue(final List<String> msgIds) throws JMSException {
        log.debug("Polling for records");
        Message message;
        for (final String string : msgIds) {
            message = reader.receive(sourceQueue, sourceQueueConfig, false);
            final String msgId = message.getJMSMessageID();
            if (!msgIds.contains(msgId)) throw new SequenceStateException("Sequence state is in an unexpected state. Please ask an MQ admin to review");
        }
        sequenceStateClient.retrieveStateInSharedTx();
        reader.commit();
    }

    protected AtomicLong getSequenceId() {
        return this.sequenceStateId;
    }

    protected List<String> getMsgIds() {
        return this.msgIds;
    }

    private void maybeFailWithTimeoutIfNotWaitAndIncrement(final List<String> msgIds) throws InterruptedException {
        if (startActionPollCount.get() >= startActionPollLimit) {
            throw new ConnectException(LogMessages.rollbackTimeout(msgIds)); // ?? sequence state exception
        }
        Thread.sleep(1000);
        startActionPollCount.incrementAndGet();
    }

    private void maybeCloseAllWorkers(final Throwable exc) {
        log.debug(" Checking to see if the failed connection should be closed.");
        if (ExceptionProcessor.isClosable(exc)) {
            closeAllWorkers();
        }
    }

    private void closeAllWorkers() {
        log.debug(" Closing connection to MQ.");
        reader.close();
        if (isExactlyOnceMode) {
            dedicated.close();
        }
    }
}