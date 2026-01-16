/**
 * Copyright 2026 IBM Corporation
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

import com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.apache.kafka.common.config.AbstractConfig;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import javax.jms.JMSContext;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for SSL-enabled MQ integration tests.
 * Provides an SSL-configured MQ container with proper certificates.
 */
public class AbstractSSLJMSContextIT {

    public static final int TCP_MQ_HOST_PORT = 9092;
    public static final int TCP_MQ_EXPOSED_PORT = 1414;
    
    public static final int REST_API_HOST_PORT = 9093;
    public static final int REST_API_EXPOSED_PORT = 9443;

    public static final String QMGR_NAME = "MYQMGR";
    public static final String CONNECTION_MODE = "client";
    public static final String SSL_CHANNEL_NAME = "DEV.APP.SVRCONN.SSL";
    public static final String NON_SSL_CHANNEL_NAME = "DEV.APP.SVRCONN";
    public static final String DEFAULT_CONNECTION_NAME = String.format("localhost(%d)", TCP_MQ_HOST_PORT);

    public static final String DEFAULT_SOURCE_QUEUE = "DEV.QUEUE.1";
    public static final String DEFAULT_STATE_QUEUE = "DEV.QUEUE.2";

    public static final String USERNAME = "app";
    public static final String ADMIN_PASSWORD = "passw0rd";
    
    // SSL certificate paths
    private static final String SSL_CERTS_DIR = "src/integration/resources/ssl-certs";
    public static final String CLIENT_KEYSTORE_PATH = SSL_CERTS_DIR + "/client.jks";
    public static final String TRUSTSTORE_PATH = SSL_CERTS_DIR + "/truststore.jks";
    public static final String KEYSTORE_PASSWORD = "password";

    @ClassRule
    @SuppressWarnings("resource") // Container is managed by JUnit @ClassRule and closed automatically
    public static GenericContainer<?> mqContainer = new GenericContainer<>(
            new ImageFromDockerfile()
                    .withFileFromPath(".", Paths.get("src/integration/resources"))
                    .withDockerfileFromBuilder(builder ->
                            builder
                                    .from(MQTestUtil.mqContainer)
                                    .user("root")
                                    .run("mkdir -p /etc/mqm/pki/keys/default /etc/mqm/pki/trust/default")
                                    .copy("ssl-certs/server.key", "/etc/mqm/pki/keys/default/key.key")
                                    .copy("ssl-certs/server-cert.pem", "/etc/mqm/pki/keys/default/key.crt")
                                    .copy("ssl-certs/ca.crt", "/etc/mqm/pki/trust/default/ca.crt")
                                    .run("chmod 600 /etc/mqm/pki/keys/default/key.key")
                                    .run("chmod 644 /etc/mqm/pki/keys/default/key.crt /etc/mqm/pki/trust/default/ca.crt")
                                    .copy("ssl-qmgr.mqsc", "/etc/mqm/99-ssl-qmgr.mqsc")
                                    .expose(1414, 9443)
                                    .build()))
            .withEnv("LICENSE", "accept")
            .withEnv("MQ_QMGR_NAME", QMGR_NAME)
            .withEnv("MQ_APP_PASSWORD", ADMIN_PASSWORD)
            .withEnv("MQ_ADMIN_PASSWORD", ADMIN_PASSWORD)
            .withExposedPorts(TCP_MQ_EXPOSED_PORT, REST_API_EXPOSED_PORT)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new com.github.dockerjava.api.model.HostConfig().withPortBindings(
                            new com.github.dockerjava.api.model.PortBinding(
                                    com.github.dockerjava.api.model.Ports.Binding.bindPort(TCP_MQ_HOST_PORT),
                                    new com.github.dockerjava.api.model.ExposedPort(TCP_MQ_EXPOSED_PORT)
                            ),
                            new com.github.dockerjava.api.model.PortBinding(
                                    com.github.dockerjava.api.model.Ports.Binding.bindPort(REST_API_HOST_PORT),
                                    new com.github.dockerjava.api.model.ExposedPort(REST_API_EXPOSED_PORT)
                            )
                    )
            ))
            .waitingFor(Wait.forLogMessage(".*AMQ5026I.*", 1));

    private JMSContext jmsContext;

    /**
     * Returns a JMS context pointing at the SSL-enabled queue manager.
     */
    protected JMSContext getJmsContext() throws Exception {
        if (jmsContext == null) {
            final MQConnectionFactory mqcf = new MQConnectionFactory();
            mqcf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqcf.setChannel(NON_SSL_CHANNEL_NAME);
            mqcf.setQueueManager(QMGR_NAME);
            mqcf.setConnectionNameList(DEFAULT_CONNECTION_NAME);

            jmsContext = mqcf.createContext(USERNAME, ADMIN_PASSWORD);
        }

        return jmsContext;
    }

    /**
     * Returns default connector properties for non-SSL connection.
     */
    public static Map<String, String> getDefaultConnectorProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", QMGR_NAME);
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", DEFAULT_CONNECTION_NAME);
        props.put("mq.channel.name", NON_SSL_CHANNEL_NAME);
        props.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("mq.record.builder", DefaultRecordBuilder.class.getCanonicalName());
        props.put("topic", "mytopic");
        return props;
    }

    /**
     * Returns connector properties configured for SSL connection.
     */
    public static Map<String, String> getSSLConnectorProperties() {
        final Map<String, String> props = getDefaultConnectorProperties();
        props.put("mq.channel.name", SSL_CHANNEL_NAME);
        props.put("mq.ssl.cipher.suite", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
        props.put("mq.user.name", USERNAME);
        props.put("mq.password", ADMIN_PASSWORD);
        props.put("mq.user.authentication.mqcsp", "true");
        return props;
    }

    public AbstractConfig getPropertiesConfig(final Map<String, String> props) {
        final AbstractConfig connectorConfig = new AbstractConfig(MQSourceConnector.CONFIGDEF, props);
        return connectorConfig;
    }
}

