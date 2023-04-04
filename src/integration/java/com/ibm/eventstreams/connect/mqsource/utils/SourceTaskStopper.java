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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.connect.source.SourceTask;


/**
 * Stops an instance of the MQSourceTask in a way that will ensure
 *  it commits the work that it has completed so far.
 *
 * This is needed for tests so that subsequent tests don't disrupt
 *  each other.
 */
public class SourceTaskStopper {

    private static ExecutorService executor = Executors.newCachedThreadPool();

    private SourceTask sourceTask;

    public SourceTaskStopper(final SourceTask task) {
        sourceTask = task;
    }

    public void run() throws InterruptedException {
        // start the poll in a background thread
        executor.submit(new PollStarter());

        // the pollstarter thread will block waiting for messages
        // that don't ever come so wait briefly before stopping
        // it from another thread
        Thread.sleep(200);
        sourceTask.stop();
    }

    class PollStarter implements Runnable {
        @Override
        public void run() {
            try {
                sourceTask.poll();
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
