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
package com.ibm.eventstreams.connect.mqsource.sequencestate;

import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class SequenceStateTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSequenceStateEquals() {

        SequenceState sequenceState1  = new SequenceState(
                1,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d50010",
                        "414d51204d59514d475220202020202033056b6401d50011",
                        "414d51204d59514d475220202020202033056b6401d50012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        SequenceState sequenceState2 = new SequenceState(
                1,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d50010",
                        "414d51204d59514d475220202020202033056b6401d50011",
                        "414d51204d59514d475220202020202033056b6401d50012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        assertThat(sequenceState1).isEqualTo(sequenceState2);
    }

    @Test
    public void testSequenceStateNotEquals() {

        SequenceState sequenceState1  = new SequenceState(
                1,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d50010",
                        "414d51204d59514d475220202020202033056b6401d50011",
                        "414d51204d59514d475220202020202033056b6401d50012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        SequenceState sequenceState2 = new SequenceState(
                2,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d50010",
                        "414d51204d59514d475220202020202033056b6401d50011",
                        "414d51204d59514d475220202020202033056b6401d50012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );

        assertThat(sequenceState1).isNotEqualTo(sequenceState2);
    }


    @Test
    public void testMapperReadValue() throws IOException {

        String input = "{" +
                         "\"sequenceId\":1," +
                         "\"messageIds\":[" +
                            "\"414d51204d59514d475220202020202033056b6401d50010\"," +
                            "\"414d51204d59514d475220202020202033056b6401d50011\"," +
                            "\"414d51204d59514d475220202020202033056b6401d50012\"" +
                        "]," +
                        "\"lastKnownState\":\"IN_FLIGHT\"" +
                        "}";

        SequenceState sequenceState = mapper.readValue(input, SequenceState.class);

        assertThat(sequenceState).isEqualTo(
                new SequenceState(
                        1,
                        new ArrayList<>(Arrays.asList(
                                "414d51204d59514d475220202020202033056b6401d50010",
                                "414d51204d59514d475220202020202033056b6401d50011",
                                "414d51204d59514d475220202020202033056b6401d50012"
                        )),
                        SequenceState.LastKnownState.IN_FLIGHT
                )
        );
    }

    @Test
    public void testMapperWriteValueAsString() throws IOException {

        SequenceState sequenceState = new SequenceState(
                1,
                new ArrayList<>(Arrays.asList(
                        "414d51204d59514d475220202020202033056b6401d50010",
                        "414d51204d59514d475220202020202033056b6401d50011",
                        "414d51204d59514d475220202020202033056b6401d50012"
                )),
                SequenceState.LastKnownState.IN_FLIGHT
        );


        assertThat(mapper.writeValueAsString(sequenceState)).isEqualTo(
                "{" +
                          "\"sequenceId\":1," +
                          "\"messageIds\":[" +
                            "\"414d51204d59514d475220202020202033056b6401d50010\"," +
                            "\"414d51204d59514d475220202020202033056b6401d50011\"," +
                            "\"414d51204d59514d475220202020202033056b6401d50012\"" +
                          "]," +
                          "\"lastKnownState\":\"IN_FLIGHT\"" +
                        "}"
        );
    }

    @Test
    public void isDeliveredAndInFlightShouldBeFalseOnEmptyConstructor() {
        SequenceState sequenceState = new SequenceState();
        assertThat(sequenceState.isDelivered()).isFalse();
        assertThat(sequenceState.isInFlight()).isFalse();
    }

    @Test
    public void isDeliveredWorks() {
        SequenceState sequenceState = new SequenceState(1,
                new ArrayList<>(),
                SequenceState.LastKnownState.DELIVERED);

        assertThat(sequenceState.isDelivered()).isTrue();
        assertThat(sequenceState.isInFlight()).isFalse();
    }
    @Test
    public void isInFlightWorks() {
        SequenceState sequenceState = new SequenceState(1,
                new ArrayList<>(),
                SequenceState.LastKnownState.IN_FLIGHT);

        assertThat(sequenceState.isInFlight()).isTrue();
        assertThat(sequenceState.isDelivered()).isFalse();
    }

}