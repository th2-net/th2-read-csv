/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.readcsv.impl;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.read.file.common.AbstractFileReader;
import com.exactpro.th2.read.file.common.DirectoryChecker;
import com.exactpro.th2.read.file.common.FileSourceWrapper;
import com.exactpro.th2.read.file.common.StreamId;
import com.exactpro.th2.read.file.common.impl.DefaultFileReader;
import com.exactpro.th2.read.file.common.impl.RecoverableBufferedReaderWrapper;
import com.exactpro.th2.readcsv.cfg.ReaderConfig;
import kotlin.Unit;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

public abstract class AbstractReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoReaderFactory.class);

    ReaderConfig configuration;
    CommonFactory commonFactory;

    public AbstractReaderFactory(ReaderConfig configuration, CommonFactory commonFactory) {
        this.configuration = configuration;
        this.commonFactory = commonFactory;
    }

    public AbstractFileReader<LineNumberReader, ?, ?> getReader() {

        MessageRouter<EventBatch> eventBatchRouter = commonFactory.getEventBatchRouter();

        Comparator<Path> pathComparator = comparing(it -> it.getFileName().toString(), String.CASE_INSENSITIVE_ORDER);
        var directoryChecker = new DirectoryChecker(
                configuration.getSourceDirectory(),
                (Path path) -> configuration.getAliases().entrySet().stream()
                        .filter(entry -> entry.getValue().getNameRegexp().matcher(path.getFileName().toString()).matches())
                        .map(it -> new StreamId(it.getKey()))
                        .collect(Collectors.toSet()),
                files -> files.sort(pathComparator),
                path -> true
        );

        var rootId = commonFactory.getRootEventId();

        return prepareReaderBuilder(eventBatchRouter, directoryChecker, rootId)
                .readFileImmediately()
                .acceptNewerFiles()
                .onError((streamId, message, ex) -> publishErrorEvent(eventBatchRouter, streamId, message, ex, rootId))
                .onSourceCorrupted((streamId, path, e) -> publishSourceCorruptedEvent(eventBatchRouter, path, streamId, e, rootId))
                .build();
    }

    protected abstract DefaultFileReader.Builder<LineNumberReader, ?, ?> prepareReaderBuilder(
            MessageRouter<EventBatch> eventBatchRouter,
            DirectoryChecker directoryChecker,
            EventID rootId
    );

    @NotNull
    protected static Unit clearHeader(HeaderHolder<?> headerHolder, StreamId streamId) {
        headerHolder.clearHeaderForAlias(streamId.getSessionAlias());
        return Unit.INSTANCE;
    }

    @NotNull
    protected static Unit publishSourceCorruptedEvent(MessageRouter<EventBatch> eventBatchRouter, Path path, StreamId streamId, Exception e, EventID rootEventId) {
        Event error = Event.start()
                .name("Corrupted source " + path + " for " + streamId.getSessionAlias())
                .type("CorruptedSource");
        return publishError(eventBatchRouter, streamId, e, error, rootEventId);
    }

    @NotNull
    protected static Unit publishErrorEvent(MessageRouter<EventBatch> eventBatchRouter, StreamId streamId, String message, Exception ex, EventID rootEventId) {
        Event error = Event.start().endTimestamp()
                .name(streamId == null ? "General error" : "Error for session alias " + streamId.getSessionAlias())
                .type("Error")
                .bodyData(EventUtils.createMessageBean(message));
        return publishError(eventBatchRouter, streamId, ex, error, rootEventId);
    }

    @NotNull
    private static Unit publishError(MessageRouter<EventBatch> eventBatchRouter, StreamId streamId, Exception ex, Event error, EventID rootEventId) {
        Throwable tmp = ex;
        while (tmp != null) {
            error.bodyData(EventUtils.createMessageBean(tmp.getMessage()));
            tmp = tmp.getCause();
        }
        try {
            eventBatchRouter.sendAll(EventBatch.newBuilder().addEvents(error.toProto(rootEventId)).build());
        } catch (Exception e) {
            LOGGER.error("Cannot send event for stream {}", streamId, e);
        }
        return Unit.INSTANCE;
    }

    protected static FileSourceWrapper<LineNumberReader> createSource(StreamId streamId, Path path) {
        try {
            return new RecoverableBufferedReaderWrapper(new LineNumberReader(Files.newBufferedReader(path)));
        } catch (IOException e) {
            return ExceptionUtils.rethrow(e);
        }
    }
}