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
package com.ibm.eventstreams.connect.mqsource.utils;

import org.jetbrains.annotations.NotNull;

import javax.jms.JMSContext;
import javax.jms.MapMessage;
import javax.jms.Message;
import java.util.ArrayList;
import java.util.List;

public class MessagesObjectMother {

    @NotNull
    public static List<Message> createAListOfMessages(JMSContext jmsContext, int numberOfMessages, String messageContent) throws Exception {
        final List<Message> messages = new ArrayList<>();
        for (int i = 1; i <= numberOfMessages; i++) {
            messages.add(jmsContext.createTextMessage(messageContent + i));
        }
        return messages;
    }

    /**
     *  messages 01-15 - valid messages
     *  message 16 - a message that the builder can't process
     *  messages 17-30 - valid messages
     */
    @NotNull
    public static List<Message> listOfMessagesButOneIsMalformed(JMSContext jmsContext) throws Exception {
        final List<Message> messages = createAListOfMessages(jmsContext, 15, "message ");
        final MapMessage invalidMessage = jmsContext.createMapMessage();
        invalidMessage.setString("test", "builder cannot convert this");
        messages.add(invalidMessage);
        for (int i = 17; i <= 30; i++) {
            messages.add(jmsContext.createTextMessage("message " + i));
        }
        return messages;
    }
}
