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
package com.ibm.eventstreams.connect.mqsource;

import static com.ibm.eventstreams.connect.mqsource.utils.SourceTaskContextObjectMother.emptyKafkaOffsetContext;
import static com.ibm.eventstreams.connect.mqsource.utils.SourceTaskContextObjectMother.kafkaContextWithOffsetGivenAs5;
import static com.ibm.eventstreams.connect.mqsource.utils.SourceTaskContextObjectMother.sourceTaskContextWithOffsetId;

import org.apache.kafka.connect.source.SourceTaskContext;

public class MQSourceTaskObjectMother {

    public static MQSourceTask getSourceTaskWithEmptyKafkaOffset() {
        SourceTaskContext contextMock = emptyKafkaOffsetContext();
        MQSourceTask connectTask = new MQSourceTask();
        connectTask.initialize(contextMock);
        return connectTask;
    }

    public static MQSourceTask getSourceTaskWithKafkaOffset() {
        SourceTaskContext contextMock = kafkaContextWithOffsetGivenAs5();
        MQSourceTask connectTask = new MQSourceTask();
        connectTask.initialize(contextMock);
        return connectTask;
    }

    public static MQSourceTask getSourceTaskWithKafkaOffset(long id) {
        SourceTaskContext contextMock = sourceTaskContextWithOffsetId(id);
        MQSourceTask connectTask = new MQSourceTask();
        connectTask.initialize(contextMock);
        return connectTask;
    }

    public static MQSourceTask getSourceTaskWithContext(SourceTaskContext context) {
        MQSourceTask connectTask = new MQSourceTask();
        connectTask.initialize(context);
        return connectTask;
    }
}
