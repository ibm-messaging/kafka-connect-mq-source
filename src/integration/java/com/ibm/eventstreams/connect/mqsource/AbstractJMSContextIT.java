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

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import javax.jms.JMSContext;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for integration tests that have a dependency on JMSContext.
 *
 *  It starts a queue manager in a test container, and uses it to create
 *  a JMSContext instance, that can be used in tests.
 */
public class AbstractJMSContextIT {

    public static final int TCP_MQ_HOST_PORT = 9090;
    public static final int TCP_MQ_EXPOSED_PORT = 1414;

    public static final int REST_API_HOST_PORT = 9091;
    public static final int REST_API_EXPOSED_PORT = 9443;

    public static final String QMGR_NAME = "MYQMGR";
    public static final String CONNECTION_MODE = "client";
    public static final String CHANNEL_NAME = "DEV.APP.SVRCONN";
    public static final String DEFAULT_CONNECTION_NAME = String.format("localhost(%d)", TCP_MQ_HOST_PORT);

    public static final String DEFAULT_SOURCE_QUEUE = "DEV.QUEUE.1";
    public static final String DEFAULT_STATE_QUEUE = "DEV.QUEUE.2";

    public static final String USERNAME = "app";
    public static final String ADMIN_PASSWORD = "passw0rd";

    @ClassRule
    public static GenericContainer<?> mqContainer = new GenericContainer<>(MQTestUtil.mqContainer)
            .withEnv("LICENSE", "accept")
            .withEnv("MQ_QMGR_NAME", QMGR_NAME)
            .withEnv("MQ_APP_PASSWORD", ADMIN_PASSWORD)
            .withEnv("MQ_ADMIN_PASSWORD", ADMIN_PASSWORD)
            .withExposedPorts(TCP_MQ_EXPOSED_PORT, REST_API_EXPOSED_PORT)
            .withCopyFileToContainer(MountableFile.forClasspathResource("no-auth-qmgr.mqsc"), "/etc/mqm/99-no-auth-qmgr.mqsc")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(TCP_MQ_HOST_PORT), new ExposedPort(TCP_MQ_EXPOSED_PORT)),
                    new PortBinding(Ports.Binding.bindPort(REST_API_HOST_PORT), new ExposedPort(REST_API_EXPOSED_PORT))
                    )
            )).waitingFor(Wait.forListeningPort());

    private JMSContext jmsContext;


    /**
     * Returns a JMS context pointing at a developer queue manager running in a
     * test container.
     */
    protected JMSContext getJmsContext() throws Exception {
        if (jmsContext == null) {
            final MQConnectionFactory mqcf = new MQConnectionFactory();
            mqcf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqcf.setChannel(CHANNEL_NAME);
            mqcf.setQueueManager(QMGR_NAME);
            mqcf.setConnectionNameList(DEFAULT_CONNECTION_NAME);

            jmsContext = mqcf.createContext(USERNAME, ADMIN_PASSWORD);
        }

        return jmsContext;
    }

    protected Map<String, String> getDefaultConnectorProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", QMGR_NAME);
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", DEFAULT_CONNECTION_NAME);
        props.put("mq.channel.name", CHANNEL_NAME);
        props.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("topic", "mytopic");
        return props;
    }

    public AbstractConfig getPropertiesConfig(Map<String, String> props) {
        final AbstractConfig connectorConfig = new AbstractConfig(MQSourceConnector.CONFIGDEF, props);
        return connectorConfig;
    }
}
