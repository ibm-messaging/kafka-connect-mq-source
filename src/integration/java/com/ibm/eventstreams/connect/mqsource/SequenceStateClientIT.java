/**
 * Copyright 2023 IBM Corporation
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

import static com.ibm.eventstreams.connect.mqsource.utils.MQQueueManagerAttrs.enableQueuePUT;
import static com.ibm.eventstreams.connect.mqsource.utils.MQQueueManagerAttrs.inhibitQueuePUT;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.browseAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.removeAllMessagesFromQueue;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.getAllMessagesFromQueue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceState;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateClient;
import com.ibm.eventstreams.connect.mqsource.sequencestate.SequenceStateException;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;

public class SequenceStateClientIT extends AbstractJMSContextIT {

    private SequenceStateClient sequenceStateClient;
    private JMSWorker shared;

    @Before
    public void createSequenceStateClient() {
        Map<String, String> props = getDefaultConnectorProperties();
        props.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        shared = new JMSWorker();
        shared.configure(getPropertiesConfig(props));
        JMSWorker dedicated = new JMSWorker();
        dedicated.configure(getPropertiesConfig(props));
        sequenceStateClient = new SequenceStateClient(DEFAULT_STATE_QUEUE, shared, dedicated);
    }

    @After
    public void closeConnectionsAndClearStateQueue() throws JMSException, IOException, NoSuchAlgorithmException, KeyManagementException {
        sequenceStateClient.closeClientConnections();
        enableQueuePUT(QMGR_NAME, REST_API_HOST_PORT, ADMIN_PASSWORD, DEFAULT_STATE_QUEUE);
        removeAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
    }

    @Test
    public void testWriteState() throws Exception {
        SequenceState sequenceState = new SequenceState(
                314,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d51010",
                        "414d51204d59514d475220202020202033056b6401d51011",
                        "414d51204d59514d475220202020202033056b6401d51012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(sequenceState);
        TextMessage firstMessage = (TextMessage) getAllMessagesFromQueue(DEFAULT_STATE_QUEUE).get(0);

        String expectedResult =
                "{" +
                  "\"sequenceId\":314," +
                  "\"messageIds\":[" +
                    "\"414d51204d59514d475220202020202033056b6401d51010\"," +
                    "\"414d51204d59514d475220202020202033056b6401d51011\"," +
                    "\"414d51204d59514d475220202020202033056b6401d51012\"" +
                  "]," +
                  "\"lastKnownState\":\"IN_FLIGHT\"" +
                "}";

        assertThat(firstMessage.getText()).isEqualTo(expectedResult);
    }

    @Test
    public void testWriteState_HasCommitted() throws JMSException {

        SequenceState sequenceState1 = new SequenceState(
                314,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d51010",
                        "414d51204d59514d475220202020202033056b6401d51011",
                        "414d51204d59514d475220202020202033056b6401d51012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(sequenceState1);
        sequenceStateClient.closeClientConnections();

        TextMessage stateMessage = (TextMessage) getAllMessagesFromQueue(DEFAULT_STATE_QUEUE).get(0);
        assertThat(stateMessage.getText().contains("FLIGHT")).isTrue();
    }

    @Test
    public void testRetrieveStateInSharedTx_ReturnsCorrectState() throws Exception {
        SequenceState sequenceState1 = new SequenceState(
                314,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d50010",
                        "414d51204d59514d475220202020202033056b6401d50011",
                        "414d51204d59514d475220202020202033056b6401d50012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(sequenceState1);

        Optional<SequenceState> sequenceState = sequenceStateClient.retrieveStateInSharedTx();
        this.shared.commit();
        assertThat(sequenceState).isNotEmpty();
        assertThat(sequenceState.get()).isEqualTo(sequenceState1);
    }

    @Test
    public void testRetrieveStateInSharedTx_IsRolledBackWithSharedJMSWorker() throws Exception {
        // setup test
        SequenceState sequenceState1 = new SequenceState(
                314,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d50010",
                        "414d51204d59514d475220202020202033056b6401d50011",
                        "414d51204d59514d475220202020202033056b6401d50012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        sequenceStateClient.write(sequenceState1);
        assertThat(browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE).size()).isEqualTo(1);

        // retrieve state Message
        Optional<SequenceState> sequenceState = sequenceStateClient.retrieveStateInSharedTx();
        assertThat(sequenceState).isNotEmpty();
        assertThat(browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE).size()).isEqualTo(0);

        // roll back
        shared.attemptRollback();
        assertThat(browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE).size()).isEqualTo(1);

    }

    @Test
    public void testRetrieveStateInSharedTx_GivenNoPreviousStateStoredOnQueue() throws Exception {
        assertThat(sequenceStateClient.retrieveStateInSharedTx()).isEmpty();
    }

    @Test
    public void test_replaceState_happyPath() throws JMSException {
        //setup state before call
        int sequenceId = 314;
        ArrayList<String> localMsgIds = new ArrayList<>(Arrays.asList(
                "414d51204d59514d475220202020202033056b6401d50010",
                "414d51204d59514d475220202020202033056b6401d50011",
                "414d51204d59514d475220202020202033056b6401d50012"
        ));
        SequenceState origState = new SequenceState(
                sequenceId,
                localMsgIds,
                SequenceState.LastKnownState.IN_FLIGHT
        );
        sequenceStateClient.write(origState);
        final List<Message> stateMsgs = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs.size()).isEqualTo(1);

        // call replace
        SequenceState newState = new SequenceState(sequenceId, localMsgIds, SequenceState.LastKnownState.DELIVERED);
        sequenceStateClient.replaceState(newState);

        // assert that the state is now delivered on the queue
        final List<Message> stateMsgs_afterReplace = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs_afterReplace.size()).isEqualTo(1);
        assertThat(sequenceStateClient.browse().get()).isEqualTo(newState);
    }

    @Test
    public void test_replaceState_isRolledBackIfWriteFails() throws JMSException, IOException, NoSuchAlgorithmException, KeyManagementException {
        //setup state before call
        int sequenceId = 314;
        ArrayList<String> localMsgIds = new ArrayList<>(Arrays.asList(
                "414d51204d59514d475220202020202033056b6401d50010",
                "414d51204d59514d475220202020202033056b6401d50011",
                "414d51204d59514d475220202020202033056b6401d50012"
        ));
        SequenceState origState = new SequenceState(
                sequenceId,
                localMsgIds,
                SequenceState.LastKnownState.IN_FLIGHT
        );
        sequenceStateClient.write(origState);
        final List<Message> stateMsgs = browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs.size()).isEqualTo(1);

        // disable queue so that put/write will fail
        inhibitQueuePUT(QMGR_NAME, REST_API_HOST_PORT, ADMIN_PASSWORD, DEFAULT_STATE_QUEUE);

        // call replace
        SequenceState newState = new SequenceState(sequenceId, localMsgIds, SequenceState.LastKnownState.DELIVERED);

        assertThrows(Exception.class, () -> sequenceStateClient.replaceState(newState));

        List<Message> stateMsgs_After = getAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThat(stateMsgs_After.size()).isEqualTo(1);
        String payload = stateMsgs_After.get(0).getBody(String.class);
        assertThat(payload.contains("DELIVERED")).isFalse();
        assertThat(payload.contains("FLIGHT")).isTrue();
    }

    @Test
    public void test_MessageCanBeConvertedToSequenceState() throws Exception{

        SequenceStateClient sequenceStateClient = new SequenceStateClient(
                DEFAULT_STATE_QUEUE,
                Mockito.mock(JMSWorker.class),
                Mockito.mock(JMSWorker.class)
        );

        SequenceState sequenceState = new SequenceState(
                314,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d51010",
                        "414d51204d59514d475220202020202033056b6401d51011",
                        "414d51204d59514d475220202020202033056b6401d51012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        MQTestUtil.putAllMessagesToQueue(DEFAULT_STATE_QUEUE, aListOfOneSequenceStateTextMessage());
        List<Message> messages = MQTestUtil.browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        SequenceState state = sequenceStateClient.messageToStateObject(messages.get(0));
        assertThat(state).isEqualTo(sequenceState);
    }

    @Test
    public void test_MessageCanNotBeCastToTextMessage() throws Exception{

        SequenceStateClient sequenceStateClient = new SequenceStateClient(
                DEFAULT_STATE_QUEUE,
                Mockito.mock(JMSWorker.class),
                Mockito.mock(JMSWorker.class)
        );

        SequenceState sequenceState = new SequenceState(
                314,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d51010",
                        "414d51204d59514d475220202020202033056b6401d51011",
                        "414d51204d59514d475220202020202033056b6401d51012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        MQTestUtil.putAllMessagesToQueue(DEFAULT_STATE_QUEUE, aListOfOneBytesMessage());
        List<Message> messages = MQTestUtil.browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThrows(SequenceStateException.class, () -> sequenceStateClient.messageToStateObject(messages.get(0)));
    }

    @Test
    public void test_MessageCanNotBeConvertedToSequenceState() throws Exception{

        SequenceStateClient sequenceStateClient = new SequenceStateClient(
                DEFAULT_STATE_QUEUE,
                Mockito.mock(JMSWorker.class),
                Mockito.mock(JMSWorker.class)
        );

        SequenceState sequenceState = new SequenceState(
                314,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d51010",
                        "414d51204d59514d475220202020202033056b6401d51011",
                        "414d51204d59514d475220202020202033056b6401d51012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        MQTestUtil.putAllMessagesToQueue(DEFAULT_STATE_QUEUE, aListOfOneStringMessage());
        List<Message> messages = MQTestUtil.browseAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        assertThrows(SequenceStateException.class, () -> sequenceStateClient.messageToStateObject(messages.get(0)));
    }

    @NotNull
    private List<Message> aListOfOneBytesMessage() throws Exception {
        return Arrays.asList(
                getJmsContext().createBytesMessage()
        );
    }

    @NotNull
    private List<Message> aListOfOneSequenceStateTextMessage() throws Exception {
        String stateAsText =
                "{" +
                "\"sequenceId\":314," +
                "\"messageIds\":[" +
                "\"414d51204d59514d475220202020202033056b6401d51010\"," +
                "\"414d51204d59514d475220202020202033056b6401d51011\"," +
                "\"414d51204d59514d475220202020202033056b6401d51012\"" +
                "]," +
                "\"lastKnownState\":\"IN_FLIGHT\"" +
                "}";
        return Arrays.asList(getJmsContext().createTextMessage(stateAsText));
    }

    @NotNull
    private List<Message> aListOfOneStringMessage() throws Exception {
        String text = "GenericTextMessage";
        return Arrays.asList(getJmsContext().createTextMessage(text));
    }

}
