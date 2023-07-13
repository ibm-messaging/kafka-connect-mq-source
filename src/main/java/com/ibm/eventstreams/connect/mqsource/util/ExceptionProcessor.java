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
package com.ibm.eventstreams.connect.mqsource.util;

import javax.jms.JMSException;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;

public class ExceptionProcessor {

    private static final Logger log = LoggerFactory.getLogger(ExceptionProcessor.class);

    
    protected static int getReason(final Throwable exc) {
        int reason = -1;

        // Try to extract the MQ reason code to see if it's a retriable exception
        Throwable t = exc.getCause();
        while (t != null) {
            if (t instanceof MQException) {
                final MQException mqe = (MQException) t;
                log.error("MQ error: CompCode {}, Reason {} {}", mqe.getCompCode(), mqe.getReason(),
                        MQConstants.lookupReasonCode(mqe.getReason()));
                reason = mqe.getReason();
                break;
            } else if (t instanceof JMSException) {
                final JMSException jmse = (JMSException) t;
                log.error("JMS exception: error code {}", jmse.getErrorCode());
            }

            t = t.getCause(); // Moves t up the stack trace until it is null.
        }
        return reason;
    }
    
    public static boolean isClosable(final Throwable exc) {
        if (getReason(exc) == MQConstants.MQRC_GET_INHIBITED) {
            log.info("A queue has the GET operation intentionally inhibited, wait for next poll.");
            return false;
        }
        log.info(" All MQ connections will be closed.");
        return true;
    }

    public static boolean isRetriable(final Throwable exc) {
        final int reason = getReason(exc);
        switch (reason) {
            // These reason codes indicate that the connection can be just retried later will probably recover
            case MQConstants.MQRC_BACKED_OUT: 
            case MQConstants.MQRC_CHANNEL_NOT_AVAILABLE:
            case MQConstants.MQRC_CONNECTION_BROKEN:
            case MQConstants.MQRC_HOST_NOT_AVAILABLE:
            case MQConstants.MQRC_NOT_AUTHORIZED:
            case MQConstants.MQRC_Q_MGR_NOT_AVAILABLE:
            case MQConstants.MQRC_Q_MGR_QUIESCING:
            case MQConstants.MQRC_Q_MGR_STOPPING:
            case MQConstants.MQRC_UNEXPECTED_ERROR:    
            case MQConstants.MQRC_GET_INHIBITED:
                log.info("JMS exception is retriable, wait for next poll.");
                return true;
        }
        log.info("JMS exception is not retriable, the connector is in a failed state.");
        return false;
    }

    public static ConnectException handleException(final Throwable exc) {
        if (isRetriable(exc)) {
            return new RetriableException(exc);
        }
        return new ConnectException(exc);
    }
}