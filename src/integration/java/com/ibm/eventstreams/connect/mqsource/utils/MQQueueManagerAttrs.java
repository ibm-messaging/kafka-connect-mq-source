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
package com.ibm.eventstreams.connect.mqsource.utils;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import org.json.JSONException;
import org.json.JSONObject;

public class MQQueueManagerAttrs {

    private static final String REQ_GET_SVRCONNS = "{"
        + "  \"type\": \"runCommand\","
        + "  \"parameters\": {"
        + "    \"command\": \"display conn(*) where (channel EQ 'DEV.APP.SVRCONN')\""
        + "  }"
        + "}";

    public static int getNumConnections(final String qmgrname, final int portnum, final String password)
            throws KeyManagementException, NoSuchAlgorithmException, IOException, JSONException {
        final String url = "https://localhost:" + portnum + "/ibmmq/rest/v2/admin/action/qmgr/" + qmgrname + "/mqsc";
        final JSONObject connectionInfo = JsonRestApi.jsonPost(url, "admin", password, REQ_GET_SVRCONNS);

        final int completionCode = connectionInfo.getInt("overallCompletionCode");
        final int reasonCode = connectionInfo.getInt("overallReasonCode");

        if (completionCode == 2 && reasonCode == 3008) {
            return 0;
        } else if (completionCode == 0 && reasonCode == 0) {
            return connectionInfo.getJSONArray("commandResponse").length();
        } else {
            return -1;
        }
    }
}
