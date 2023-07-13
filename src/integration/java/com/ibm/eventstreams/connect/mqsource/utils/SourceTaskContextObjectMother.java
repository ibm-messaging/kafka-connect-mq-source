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
package com.ibm.eventstreams.connect.mqsource.utils;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;

public class SourceTaskContextObjectMother {

    @NotNull
    public static SourceTaskContext emptyKafkaOffsetContext() {
        return createContext(Collections.emptyMap());
    }

    @NotNull
    public static SourceTaskContext kafkaContextWithOffsetGivenAs5() {
        Map<String, Object> map = new HashMap<>();
        map.put("sequence-id", 5L);
        return createContext(map);
    }

    @NotNull
    public static SourceTaskContext SourceTaskContextWithOffsetId(long id) {
        Map<String, Object> map = new HashMap<>();
        map.put("sequence-id", id);
        return createContext(map);
    }

    @NotNull
    public static SourceTaskContext sourceTaskContextWithOffsetId(long id) {
        Map<String, Object> map = new HashMap<>();
        map.put("sequence-id", id);
        return createContext(map);
    }

    @NotNull
    private static SourceTaskContext createContext(Map<String, Object> map) {
        SourceTaskContext contextMock = Mockito.mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReaderMock = Mockito.mock(OffsetStorageReader.class);

        Mockito.when(offsetStorageReaderMock.offset(any())).thenReturn(map);
        Mockito.when(contextMock.offsetStorageReader()).thenReturn(offsetStorageReaderMock);

        return contextMock;
    }

}
