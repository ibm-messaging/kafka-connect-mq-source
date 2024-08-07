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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Objects;


public class SequenceState {

    private long sequenceId;
    private List<String> messageIds;
    private LastKnownState lastKnownState;

    public SequenceState(final long sequenceId, final List<String> messageIds, final LastKnownState lastKnownState) {
        this.sequenceId = sequenceId;
        this.messageIds = messageIds;
        this.lastKnownState = lastKnownState;
    }

    public SequenceState() {
    }

    public long getSequenceId() {
        return this.sequenceId;
    }

    public List<String> getMessageIds() {
        return this.messageIds;
    }

    public LastKnownState getLastKnownState() {
        return this.lastKnownState;
    }

    public boolean equals(final Object o) {
        if (o == this) return true;
        if (!(o instanceof SequenceState)) return false;
        final SequenceState other = (SequenceState) o;
        if (!other.canEqual((Object) this)) return false;
        if (this.getSequenceId() != other.getSequenceId()) return false;
        final Object thismessageIds = this.getMessageIds();
        final Object othermessageIds = other.getMessageIds();
        if (!Objects.equals(thismessageIds, othermessageIds))
            return false;
        final Object thislastKnownState = this.getLastKnownState();
        final Object otherlastKnownState = other.getLastKnownState();
        if (!Objects.equals(thislastKnownState, otherlastKnownState))
            return false;
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof SequenceState;
    }

    public int hashCode() {
        final int prime = 59;
        int result = 1;
        final long sequenceId = this.getSequenceId();
        result = result * prime + (int) (sequenceId >>> 32 ^ sequenceId);
        final Object messageIds = this.getMessageIds();
        result = result * prime + (messageIds == null ? 43 : messageIds.hashCode());
        final Object lastKnownState = this.getLastKnownState();
        result = result * prime + (lastKnownState == null ? 43 : lastKnownState.hashCode());
        return result;
    }

    public String toString() {
        return "SequenceState(sequenceId=" + this.getSequenceId() + ", messageIds=" + this.getMessageIds() + ", lastKnownState=" + this.getLastKnownState() + ")";
    }

    public enum LastKnownState {
        IN_FLIGHT, DELIVERED;
    }

    public final static long DEFAULT_SEQUENCE_ID = 0;

    @JsonIgnore
    public final boolean isDelivered() {
        return this.lastKnownState == LastKnownState.DELIVERED;
    }

    @JsonIgnore
    public final boolean isInFlight() {
        return this.lastKnownState == LastKnownState.IN_FLIGHT;
    }

}

