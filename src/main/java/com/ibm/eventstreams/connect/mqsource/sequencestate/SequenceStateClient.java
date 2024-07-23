/**
 * Copyright 2023, 2024 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource.sequencestate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.eventstreams.connect.mqsource.JMSWorker;
import com.ibm.eventstreams.connect.mqsource.util.LogMessages;
import com.ibm.eventstreams.connect.mqsource.util.QueueConfig;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Optional;

public class SequenceStateClient {
    private static final Logger log = LoggerFactory.getLogger(SequenceStateClient.class);

    private final String stateQueueName;
    private final QueueConfig stateQueueConfig;
    private final ObjectMapper mapper;
    private final JMSWorker sharedJMSWorker;
    private final JMSWorker dedicatedJMSWorker;

    public SequenceStateClient(final String stateQueue, final JMSWorker sharedJMSWorker, final JMSWorker dedicatedJMSWorker) {
        this.stateQueueName = stateQueue;
        this.sharedJMSWorker = sharedJMSWorker;
        this.dedicatedJMSWorker = dedicatedJMSWorker;
        stateQueueConfig = new QueueConfig(true, false);
        mapper = new ObjectMapper();
    }

    public Optional<SequenceState> browse() throws JMSRuntimeException, JMSException {
        final Optional<Message> message = dedicatedJMSWorker.browse(stateQueueName);
        if (message.isPresent()) {
            final SequenceState sequenceState = messageToStateObject(message.get());
            log.debug(MessageFormat.format("State message read (non-destructive GET) from queue: {0}", stateQueueName));
            return Optional.of(sequenceState);
        } else {
            return Optional.empty();
        }
    }

    public Optional<SequenceState> retrieveStateInSharedTx() throws JMSRuntimeException, JMSException {
        final Message message = sharedJMSWorker.receive(stateQueueName, stateQueueConfig, false);
        if (message == null) {
            return Optional.empty();
        }
        final SequenceState sequenceState = messageToStateObject(message);
        log.debug(MessageFormat.format("State message read (destructive GET)  from queue: {0}", stateQueueName));
        return Optional.of(sequenceState);
    }

    public SequenceState write(final SequenceState sequenceState) throws JMSException {
        final String json;
        try { 
            json = mapper.writeValueAsString(sequenceState);
        } catch (final JsonProcessingException e) {
            throw convertToSequenceStateExceptionWithErrorMessage(e, LogMessages.JSON_PARSING_ERROR);
        }
        dedicatedJMSWorker.putTextMessage(json, stateQueueName);
        dedicatedJMSWorker.commit();
        log.debug(MessageFormat.format("State message written to queue: {0}", stateQueueName));
        return sequenceState;
    }

    public SequenceState replaceState(final SequenceState newState) throws  JMSException {
        dedicatedJMSWorker.receive(stateQueueName, stateQueueConfig, false);
        log.debug(MessageFormat.format("State message read (destructive GET)  from queue: {0}", stateQueueName));
        try {
            dedicatedJMSWorker.putTextMessage(mapper.writeValueAsString(newState), stateQueueName);
            log.debug(MessageFormat.format("State message written to queue: {0}", stateQueueName));
        } catch (final JsonProcessingException e) {
            throw convertToSequenceStateExceptionWithErrorMessage(e, LogMessages.JSON_PARSING_ERROR);
        }
        dedicatedJMSWorker.commit();
        return newState;
    }

    /**
     *  Validates that the state queue has either zero or one message. Throws error if
     *  a) the queue contains more than one message
     *  b) the queue can not be browsed
     * @throws JMSException
     */
    public void validateStateQueue() throws JMSException {
        final boolean invalid;
        invalid = dedicatedJMSWorker.queueHoldsMoreThanOneMessage(stateQueueName);
        if (invalid) throw new SequenceStateException("State Queue holds more than one message.");
    }

    public void closeClientConnections() {
        dedicatedJMSWorker.stop();
        sharedJMSWorker.stop();
    }

    public Optional<Long> getSequenceFromKafkaOffset(final SourceTaskContext context, final String offsetIdentifier, final Map<String, String> sourceQueuePartition) {
        final OffsetStorageReader offsetStorageReader = context.offsetStorageReader();
        final Map<String, ?> kafkaConnectOffset = offsetStorageReader.offset(sourceQueuePartition);
        return Optional.ofNullable(kafkaConnectOffset).map(offSetMap -> (Long) offSetMap.get(offsetIdentifier));
    }

    public SequenceState messageToStateObject(final Message message) throws JMSException {
        final TextMessage textMessage;
        final SequenceState sequenceState;
        try {
            textMessage = (TextMessage) message;
            sequenceState = mapper.readValue(textMessage.getText(), SequenceState.class);
        } catch (final ClassCastException e) {
            throw convertToSequenceStateExceptionWithErrorMessage(e, LogMessages.CASTING_MQ_SEQ_STATE_TO_TEXTMSG_ERROR);
        } catch (final JsonProcessingException e) {
            throw convertToSequenceStateExceptionWithErrorMessage(e, LogMessages.JSON_PARSING_ERROR);
        }
        return sequenceState;
    }

    private SequenceStateException convertToSequenceStateExceptionWithErrorMessage(final Exception e, final String message) {
        log.error(message, e);
        return new SequenceStateException(message, e);
    }
}