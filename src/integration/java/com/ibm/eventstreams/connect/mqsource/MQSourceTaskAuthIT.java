/**
 * Copyright 2022, 2023, 2024 IBM Corporation
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

import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithEmptyKafkaOffset;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.ibm.eventstreams.connect.mqsource.utils.MQQueueManagerAttrs;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

@SuppressWarnings("resource")
public class MQSourceTaskAuthIT {

    private static final String QMGR_NAME = "MYAUTHQMGR";
    private static final String QUEUE_NAME = "DEV.QUEUE.1";
    private static final String CHANNEL_NAME = "DEV.APP.SVRCONN";
    private static final String APP_PASSWORD = "MySuperSecretPassword";
    private static final String ADMIN_PASSWORD = "MyAdminPassword";

    private static final int TCP_MQ_HOST_PORT = 9090;
    private static final int TCP_MQ_EXPOSED_PORT = 1414;

    private static final int REST_API_HOST_PORT = 9091;
    private static final int REST_API_EXPOSED_PORT = 9443;

    @ClassRule
     public static GenericContainer<?> mqContainer = new GenericContainer<>(MQTestUtil.mqContainer)
            .withEnv("LICENSE", "accept")
            .withEnv("MQ_QMGR_NAME", QMGR_NAME)
            .withEnv("MQ_APP_PASSWORD", APP_PASSWORD)
            .withEnv("MQ_ADMIN_PASSWORD", ADMIN_PASSWORD)
            .withExposedPorts(TCP_MQ_EXPOSED_PORT, REST_API_EXPOSED_PORT)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(TCP_MQ_HOST_PORT), new ExposedPort(TCP_MQ_EXPOSED_PORT)),
                            new PortBinding(Ports.Binding.bindPort(REST_API_HOST_PORT), new ExposedPort(REST_API_EXPOSED_PORT))
                    )
                )).waitingFor(Wait.forListeningPort());

    private Map<String, String> getConnectorProps() {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("mq.queue.manager", QMGR_NAME);
        connectorProps.put("mq.connection.mode", "client");
        connectorProps.put("mq.connection.name.list", "localhost(" + mqContainer.getMappedPort(1414).toString() + ")");
        connectorProps.put("mq.channel.name", CHANNEL_NAME);
        connectorProps.put("mq.queue", QUEUE_NAME);
        connectorProps.put("mq.user.authentication.mqcsp", "true");
        connectorProps.put("mq.user.name", "app");
        connectorProps.put("mq.password", APP_PASSWORD);
        connectorProps.put("mq.message.body.jms", "false");
        connectorProps.put("mq.record.builder", "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
        connectorProps.put("topic", "mytopic");
        return connectorProps;
    }

    @Test
    public void testAuthenticatedQueueManager() throws Exception {

        final MQSourceTask newConnectTask = getSourceTaskWithEmptyKafkaOffset();
        newConnectTask.start(getConnectorProps());

        final MQMessage message1 = new MQMessage();
        message1.writeString("hello");
        final MQMessage message2 = new MQMessage();
        message2.writeString("world");
        putAllMessagesToQueue(Arrays.asList(message1, message2));

        final List<SourceRecord> kafkaMessages = newConnectTask.poll();
        assertEquals(2, kafkaMessages.size());
        for (final SourceRecord kafkaMessage : kafkaMessages) {
            assertNull(kafkaMessage.key());
            assertEquals(Schema.OPTIONAL_BYTES_SCHEMA, kafkaMessage.valueSchema());

            newConnectTask.commitRecord(kafkaMessage);
        }

        assertArrayEquals("hello".getBytes(), (byte[]) kafkaMessages.get(0).value());
        assertArrayEquals("world".getBytes(), (byte[]) kafkaMessages.get(1).value());

        final SourceTaskStopper stopper = new SourceTaskStopper(newConnectTask);
        stopper.run();
    }

    @Test
    public void verifyJmsConnClosed() throws Exception {

        final int restApiPortNumber = mqContainer.getMappedPort(9443);

        // count number of connections to the qmgr at the start
        final int numQmgrConnectionsBefore = MQQueueManagerAttrs.getNumConnections(QMGR_NAME, restApiPortNumber,
                ADMIN_PASSWORD);

        // start the source connector so that it connects to the qmgr
        final MQSourceTask connectTask = getSourceTaskWithEmptyKafkaOffset();
        connectTask.start(getConnectorProps());

        // count number of connections to the qmgr now - it should have increased
        final int numQmgrConnectionsDuring = MQQueueManagerAttrs.getNumConnections(QMGR_NAME, restApiPortNumber,
                ADMIN_PASSWORD);

        // stop the source connector so it disconnects from the qmgr
        connectTask.stop();

        // count number of connections to the qmgr now - it should have decreased
        final int numQmgrConnectionsAfter = MQQueueManagerAttrs.getNumConnections(QMGR_NAME, restApiPortNumber,
                ADMIN_PASSWORD);

        // verify number of connections changed as expected
        assertTrue("connections should have increased after starting the source task",
                numQmgrConnectionsDuring > numQmgrConnectionsBefore);
        assertTrue("connections should have decreased after calling stop()",
                numQmgrConnectionsAfter < numQmgrConnectionsDuring);

        // cleanup
        final SourceTaskStopper stopper = new SourceTaskStopper(connectTask);
        stopper.run();
    }

    private void putAllMessagesToQueue(final List<MQMessage> messages) throws MQException {
        final Hashtable<Object, Object> props = new Hashtable<>();
        props.put(MQConstants.HOST_NAME_PROPERTY, "localhost");
        props.put(MQConstants.PORT_PROPERTY, mqContainer.getMappedPort(1414));
        props.put(MQConstants.CHANNEL_PROPERTY, CHANNEL_NAME);
        props.put(MQConstants.USER_ID_PROPERTY, "app");
        props.put(MQConstants.PASSWORD_PROPERTY, APP_PASSWORD);

        final MQQueueManager qmgr = new MQQueueManager(QMGR_NAME, props);

        final MQQueue q = qmgr.accessQueue(QUEUE_NAME, MQConstants.MQOO_OUTPUT);

        for (final MQMessage message : messages) {
            q.put(message);
        }
    }
}
