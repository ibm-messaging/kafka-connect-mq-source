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
package com.ibm.eventstreams.connect.mqsource.builders;

import com.ibm.eventstreams.connect.mqsource.MQSourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RecordBuilderFactory {

    private static final Logger log = LoggerFactory.getLogger(RecordBuilderFactory.class);

    public static RecordBuilder getRecordBuilder(final Map<String, String> props) {
        return getRecordBuilder(
                props.get(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER),
                props
        );
    }

    protected static RecordBuilder getRecordBuilder(final String builderClass, final Map<String, String> props) {

        final RecordBuilder builder;

        try {
            final Class<? extends RecordBuilder> c = Class.forName(builderClass).asSubclass(RecordBuilder.class);
            builder = c.newInstance();
            builder.configure(props);
        } catch (ClassNotFoundException | ClassCastException | IllegalAccessException | InstantiationException | NullPointerException exc) {
            log.error("Could not instantiate message builder {}", builderClass);
            throw new RecordBuilderException("Could not instantiate message builder", exc);
        }

        return builder;
    }
}
