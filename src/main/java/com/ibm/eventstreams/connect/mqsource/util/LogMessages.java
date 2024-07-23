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

import java.text.MessageFormat;
import java.util.List;

public class LogMessages {
    
    // Error messages

    public final static String JSON_PARSING_ERROR = "The sequence state stored on the MQ State queue failed to parse as to a sequence state locally." +
        "The state queue should contain at most one message containing a valid JSON representation of a sequence state" +
        "e.g.\n" +
        "{\n" + 
        "    \"sequenceId\":314,\n" +
        "    \"messageIds\":[\n" +
        "        \"414d51204d59514d475220202020202033056b6401d51010\",\n" +
        "        \"414d51204d59514d475220202020202033056b6401d51011\",\n" +
        "        \"414d51204d59514d475220202020202033056b6401d51012\"\n" +
        "    ],\n" +
        "    \"lastKnownState\":\"IN_FLIGHT\"\n" +
        "}\n" +
        "An admin needs to inspect the state queue before the connector is restarted.";

    public final static String CASTING_MQ_SEQ_STATE_TO_TEXTMSG_ERROR = "The sequence state stored on the MQ State queue failed to cast to a TextMessage." +
        " The state queue should only contain messages of the TextMessage type." +
        " An admin needs to inspect the state queue before the connector is restarted.";

    public final static String UNEXPECTED_MESSAGE_ON_STATE_QUEUE = "Unexpected State on the MQ state queue, please review the messages on the state queue and refer to the readme for further guidance: ";

    public final static String UNEXPECTED_EXCEPTION = "An unexpected exception has been thrown, please raise a support case or github issue including the connector logs and configuration.";

    public final static String rollbackTimeout(final List<String> msgIds) {
        return MessageFormat.format(
            "The connector has tried to get the messages with the following ids: \n{0}\n " +
            "for 5 minutes and these messages are not available on the source queue. To adhere to the exactly once delivery the " +
            "connector has been put in a failed state to allow the cause to be investigated. An admin needs to inspect the state " +
            "queue and source queue before the connector is restarted.",
        msgIds);
    }
}
