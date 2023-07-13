/**
 * Copyright 2022, 2023 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

public class MQQueueManagerAttrs {

    private static final Logger log = LoggerFactory.getLogger(MQQueueManagerAttrs.class);

    public static String generateGetPutMqscCommand(final String queueName, final boolean enable, final boolean get) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("type", "runCommand");

        ObjectNode parametersNode = mapper.createObjectNode();
        String operation = get ? "GET" : "PUT";
        String property = enable ? "ENABLED" : "DISABLED";
        parametersNode.put("command", "ALTER QLOCAL(\'" + queueName + "\') "+operation+"("+property+")" );

        rootNode.put("parameters", parametersNode);
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
    };

    private static final String REQ_GET_SVRCONNS = "{"
        + "  \"type\": \"runCommand\","
        + "  \"parameters\": {"
        + "    \"command\": \"display conn(*) where (channel EQ 'DEV.APP.SVRCONN')\""
        + "  }"
        + "}";

    private static final String REQ_STOP_QUEUE = "{"
            + "  \"type\": \"runCommand\","
            + "  \"parameters\": {"
            + "    \"command\": \"STOP CHANNEL('DEV.APP.SVRCONN') MODE(QUIESCE)\""
            + "  }"
            + "}";

    private static final String REQ_START_QUEUE = "{"
            + "  \"type\": \"runCommand\","
            + "  \"parameters\": {"
            + "    \"command\": \"START CHANNEL('DEV.APP.SVRCONN')\""
            + "  }"
            + "}";

    private static final String INHIBIT_GET_ON_QUEUE = "{"
            + "  \"type\": \"runCommand\","
            + "  \"parameters\": {"
            + "    \"command\": \"ALTER QLOCAL('DEV.QUEUE.1') GET(DISABLED)\""
            + "  }"
            + "}";

    private static final String ENABLE_QUEUE = "{"
            + "  \"type\": \"runCommand\","
            + "  \"parameters\": {"
            + "    \"command\": \"ALTER QLOCAL('DEV.QUEUE.1') GET(ENABLED)\""
            + "  }"
            + "}";


    public static int inhibitQueueGET(final String qmgrname, final int portnum, final String password)
            throws IOException, NoSuchAlgorithmException, KeyManagementException {
        return sendCommand(qmgrname, portnum, password, INHIBIT_GET_ON_QUEUE);
    }

    public static int inhibitQueuePUT(final String qmgrname, final int portnum, final String password, final String queueName)
            throws IOException, NoSuchAlgorithmException, KeyManagementException {
        return sendCommand(qmgrname, portnum, password, generateGetPutMqscCommand(queueName, false, false));
    }

    public static int enableQueueGET(final String qmgrname, final int portnum, final String password)
            throws IOException, NoSuchAlgorithmException, KeyManagementException {
        return sendCommand(qmgrname, portnum, password, ENABLE_QUEUE);
    }

    public static int enableQueuePUT(final String qmgrname, final int portnum, final String password, final String queueName)
            throws IOException, NoSuchAlgorithmException, KeyManagementException {
        return sendCommand(qmgrname, portnum, password, generateGetPutMqscCommand(queueName, true, false));
    }

    public static int startChannel(final String qmgrname, final int portnum, final String password)
            throws IOException, NoSuchAlgorithmException, KeyManagementException {
        return sendCommand(qmgrname, portnum, password, REQ_START_QUEUE);
    }

    public static int stopChannel(final String qmgrname, final int portnum, final String password)
            throws IOException, NoSuchAlgorithmException, KeyManagementException {
        return sendCommand(qmgrname, portnum, password, REQ_STOP_QUEUE);
    }

    public static int getNumConnections(final String qmgrname, final int portnum, final String password)
            throws KeyManagementException, NoSuchAlgorithmException, IOException, JSONException {
        return sendCommand(qmgrname, portnum, password, REQ_GET_SVRCONNS);
    }

    private static int sendCommand(String qmgrname, int portnum, String password, String request) throws IOException, KeyManagementException, NoSuchAlgorithmException {
        final String url = "https://localhost:" + portnum + "/ibmmq/rest/v2/admin/action/qmgr/" + qmgrname + "/mqsc";
        final JSONObject commandResult = JsonRestApi.jsonPost(url, "admin", password, request);

        log.debug("result = " + commandResult);

        final int completionCode = commandResult.getInt("overallCompletionCode");
        final int reasonCode = commandResult.getInt("overallReasonCode");

        if (completionCode == 2 && reasonCode == 3008) {
            return 0;
        } else if (completionCode == 0 && reasonCode == 0) {
            return commandResult.getJSONArray("commandResponse").length();
        } else {
            return -1;
        }
    }
}
