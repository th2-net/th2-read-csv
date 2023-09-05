/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.readcsv;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.readcsv.cfg.ReaderConfig;
import com.exactpro.th2.readcsv.impl.ProtoReaderFactory;
import com.exactpro.th2.readcsv.impl.ReaderAbstractFactory;
import com.exactpro.th2.readcsv.impl.TransportReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Deque<AutoCloseable> toDispose = new ArrayDeque<>();
        var lock = new ReentrantLock();
        var condition = lock.newCondition();
        configureShutdownHook(toDispose, lock, condition);

        try {
            CommonMetrics.setLiveness(true);
            CommonFactory commonFactory = CommonFactory.createFromArguments(args);
            toDispose.add(commonFactory);

            ReaderConfig configuration = commonFactory.getCustomConfiguration(ReaderConfig.class, ReaderConfig.MAPPER);
            if (configuration.getPullingInterval().isNegative()) {
                throw new IllegalArgumentException("Pulling interval " + configuration.getPullingInterval() + " must not be negative");
            }

            ReaderAbstractFactory readerFactory;
            if (configuration.isUseTransport()) {
                readerFactory = new TransportReaderFactory(configuration, commonFactory);
            } else {
                readerFactory = new ProtoReaderFactory(configuration, commonFactory);
            }

            var reader = readerFactory.getReader();

            toDispose.add(reader);

            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            toDispose.add(() -> {
                executorService.shutdown();
                if (executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warn("Cannot shutdown executor for 5 seconds");
                    executorService.shutdownNow();
                }
            });

            ScheduledFuture<?> future = executorService.scheduleWithFixedDelay(reader::processUpdates, 0, configuration.getPullingInterval().toMillis(), TimeUnit.MILLISECONDS);
            CommonMetrics.setReadiness(true);

            awaitShutdown(lock, condition);
            future.cancel(true);
        } catch (Exception e) {
            LOGGER.error("Cannot initiate CSV reader", e);
            System.exit(2);
        }
    }

    private static void configureShutdownHook(Deque<AutoCloseable> resources, ReentrantLock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown start");
            CommonMetrics.setReadiness(false);
            try {
                lock.lock();
                condition.signalAll();
            } finally {
                lock.unlock();
            }
            resources.descendingIterator().forEachRemaining(resource -> {
                try {
                    resource.close();
                } catch (Exception e) {
                    LOGGER.error("Cannot close resource {}", resource.getClass(), e);
                }
            });

            CommonMetrics.setLiveness(false);
            LOGGER.info("Shutdown end");
        }, "Shutdown hook"));
    }

    private static void awaitShutdown(ReentrantLock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            LOGGER.info("Wait shutdown");
            condition.await();
            LOGGER.info("App shutdown");
        } finally {
            lock.unlock();
        }
    }
}