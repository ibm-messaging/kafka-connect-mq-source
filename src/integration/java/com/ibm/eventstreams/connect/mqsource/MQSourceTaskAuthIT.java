/**
 * Copyright 2022 IBM Corporation
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.WaitingConsumer;

import com.ibm.eventstreams.connect.mqsource.utils.MQQueueManagerAttrs;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

public class MQSourceTaskAuthIT {

    private static final String QMGR_NAME = "MYAUTHQMGR";
    private static final String QUEUE_NAME = "DEV.QUEUE.2";
    private static final String CHANNEL_NAME = "DEV.APP.SVRCONN";
    private static final String APP_PASSWORD = "MySuperSecretPassword";
    private static final String ADMIN_PASSWORD = "MyAdminPassword";


    @ClassRule
    public static GenericContainer<?> MQ_CONTAINER = new GenericContainer<>("icr.io/ibm-messaging/mq:latest")
        .withEnv("LICENSE", "accept")
        .withEnv("MQ_QMGR_NAME", QMGR_NAME)
        .withEnv("MQ_APP_PASSWORD", APP_PASSWORD)
        .withEnv("MQ_ADMIN_PASSWORD", ADMIN_PASSWORD)
        .withExposedPorts(1414, 9443);


    private Map<String, String> getConnectorProps() {
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("mq.queue.manager", QMGR_NAME);
        connectorProps.put("mq.connection.mode", "client");
        connectorProps.put("mq.connection.name.list", "localhost(" + MQ_CONTAINER.getMappedPort(1414).toString() + ")");
        connectorProps.put("mq.channel.name", CHANNEL_NAME);
        connectorProps.put("mq.queue", QUEUE_NAME);
        connectorProps.put("mq.user.authentication.mqcsp", "true");
        connectorProps.put("mq.user.name", "app");
        connectorProps.put("mq.password", APP_PASSWORD);
        connectorProps.put("mq.message.body.jms", "false");
        connectorProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        return connectorProps;
    }

    @Test
    public void testAuthenticatedQueueManager() throws Exception {
        waitForQueueManagerStartup();

        MQSourceTask newConnectTask = new MQSourceTask();
        newConnectTask.start(getConnectorProps());

        MQMessage message1 = new MQMessage();
        message1.writeString("hello");
        MQMessage message2 = new MQMessage();
        message2.writeString("world");
        putAllMessagesToQueue(Arrays.asList(message1, message2));

        List<SourceRecord> kafkaMessages = newConnectTask.poll();
        assertEquals(2, kafkaMessages.size());
        for (SourceRecord kafkaMessage : kafkaMessages) {
            assertNull(kafkaMessage.key());
            assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, kafkaMessage.valueSchema());

            newConnectTask.commitRecord(kafkaMessage);
        }

        assertArrayEquals("hello".getBytes(), (byte[]) kafkaMessages.get(0).value());
        assertArrayEquals("world".getBytes(), (byte[]) kafkaMessages.get(1).value());

        SourceTaskStopper stopper = new SourceTaskStopper(newConnectTask);
        stopper.run();
    }



    @Test
    public void verifyJmsConnClosed() throws Exception {

        int restApiPortNumber = MQ_CONTAINER.getMappedPort(9443);

        // count number of connections to the qmgr at the start
        int numQmgrConnectionsBefore = MQQueueManagerAttrs.getNumConnections(QMGR_NAME, restApiPortNumber, ADMIN_PASSWORD);

        // start the source connector so that it connects to the qmgr
        MQSourceTask connectTask = new MQSourceTask();
        connectTask.start(getConnectorProps());

        // count number of connections to the qmgr now - it should have increased
        int numQmgrConnectionsDuring = MQQueueManagerAttrs.getNumConnections(QMGR_NAME, restApiPortNumber, ADMIN_PASSWORD);

        // stop the source connector so it disconnects from the qmgr
        connectTask.stop();

        // count number of connections to the qmgr now - it should have decreased
        int numQmgrConnectionsAfter = MQQueueManagerAttrs.getNumConnections(QMGR_NAME, restApiPortNumber, ADMIN_PASSWORD);

        // verify number of connections changed as expected
        assertTrue("connections should have increased after starting the source task",
                   numQmgrConnectionsDuring > numQmgrConnectionsBefore);
        assertTrue("connections should have decreased after calling stop()",
                   numQmgrConnectionsAfter < numQmgrConnectionsDuring);

        // cleanup
        SourceTaskStopper stopper = new SourceTaskStopper(connectTask);
        stopper.run();
    }



    private void waitForQueueManagerStartup() throws TimeoutException {
        WaitingConsumer logConsumer = new WaitingConsumer();
        MQ_CONTAINER.followOutput(logConsumer);
        logConsumer.waitUntil(logline -> logline.getUtf8String().contains("AMQ5975I"));
    }


    private void putAllMessagesToQueue(List<MQMessage> messages) throws MQException {
        Hashtable<Object, Object> props = new Hashtable<>();
        props.put(MQConstants.HOST_NAME_PROPERTY, "localhost");
        props.put(MQConstants.PORT_PROPERTY, MQ_CONTAINER.getMappedPort(1414));
        props.put(MQConstants.CHANNEL_PROPERTY, CHANNEL_NAME);
        props.put(MQConstants.USER_ID_PROPERTY, "app");
        props.put(MQConstants.PASSWORD_PROPERTY, APP_PASSWORD);

        MQQueueManager qmgr = new MQQueueManager(QMGR_NAME, props);

        MQQueue q = qmgr.accessQueue(QUEUE_NAME, MQConstants.MQOO_OUTPUT);

        for (MQMessage message : messages) {
            q.put(message);
        }
    }
}
