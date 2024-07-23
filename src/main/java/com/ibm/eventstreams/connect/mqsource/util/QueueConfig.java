/**
 * Copyright 2023, 2024 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource.util;

import com.ibm.mq.jms.MQQueue;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.JMSException;
import java.util.Map;

import static com.ibm.eventstreams.connect.mqsource.MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS;
import static com.ibm.eventstreams.connect.mqsource.MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_MQMD_READ;


public class QueueConfig {
    final private boolean mqMessageBodyJms;
    final private boolean mqMessageMqmdRead;

    public QueueConfig(final Map<String, String> props) {
        this(
                Boolean.parseBoolean(props.get(CONFIG_NAME_MQ_MESSAGE_BODY_JMS)),
                Boolean.parseBoolean(props.get(CONFIG_NAME_MQ_MESSAGE_MQMD_READ))
        );
    }

    public QueueConfig(final Boolean mqMessageBodyJms, final Boolean mqMessageMqmdRead) {
        this.mqMessageBodyJms = mqMessageBodyJms;
        this.mqMessageMqmdRead = mqMessageMqmdRead;
    }

    public MQQueue applyToQueue(final MQQueue queue) throws JMSException {
        if (mqMessageBodyJms) {
            queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_JMS);
        } else {
            queue.setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
        }

        if (mqMessageMqmdRead) queue.setBooleanProperty(WMQConstants.WMQ_MQMD_READ_ENABLED, true);
        return queue;
    }

    public boolean isMqMessageBodyJms() {
        return this.mqMessageBodyJms;
    }

    public boolean isMqMessageMqmdRead() {
        return this.mqMessageMqmdRead;
    }
}
