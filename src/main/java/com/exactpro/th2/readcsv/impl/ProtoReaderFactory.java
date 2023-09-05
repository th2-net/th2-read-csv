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

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.read.file.common.AbstractFileReader;
import com.exactpro.th2.read.file.common.DirectoryChecker;
import com.exactpro.th2.read.file.common.MovedFileTracker;
import com.exactpro.th2.read.file.common.StreamId;
import com.exactpro.th2.read.file.common.impl.ProtoDefaultFileReader;
import com.exactpro.th2.read.file.common.state.impl.InMemoryReaderState;
import com.exactpro.th2.readcsv.cfg.ReaderConfig;
import com.google.protobuf.ByteString;
import kotlin.Unit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.LineNumberReader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ProtoReaderFactory extends ReaderAbstractFactory {

    public ProtoReaderFactory(ReaderConfig configuration, CommonFactory commonFactory) {
        super(configuration, commonFactory);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoReaderFactory.class);

    @Override
    protected AbstractFileReader<LineNumberReader, ?, ?> buildReader(MessageRouter<EventBatch> eventBatchRouter, DirectoryChecker directoryChecker, EventID rootId) {
        var headerHolder = new ProtoHeaderHolder(configuration.getAliases());
        var router = commonFactory.getMessageRouterRawBatch();

        return new ProtoDefaultFileReader.Builder<>(
                configuration.getCommon(),
                directoryChecker,
                new ProtoCsvContentParser(configuration.getAliases()),
                new MovedFileTracker(configuration.getSourceDirectory()),
                new InMemoryReaderState(),
                streamId -> commonFactory.newMessageIDBuilder().build(),
                ReaderAbstractFactory::createSource
        )
                .readFileImmediately()
                .acceptNewerFiles()
                .onSourceFound((streamId, path) -> clearHeader(headerHolder, streamId))
                .onContentRead((streamId, path, builders) -> attachHeaderOrHold(headerHolder, streamId, builders, configuration))
                .onStreamData((streamId, builders) -> publishMessages(router, streamId, builders))
                .onError((streamId, message, ex) -> publishErrorEvent(eventBatchRouter, streamId, message, ex, rootId))
                .onSourceCorrupted((streamId, path, e) -> publishSourceCorruptedEvent(eventBatchRouter, path, streamId, e, rootId))
                .build();
    }

    @NotNull
    private static Collection<? extends RawMessage.Builder> attachHeaderOrHold(
            HeaderHolder<ByteString> headerHolder,
            StreamId streamId,
            Collection<? extends RawMessage.Builder> builders,
            ReaderConfig cfg
    ) {
        String sessionAlias = streamId.getSessionAlias();
        HeaderInfo<ByteString> headerForAlias = headerHolder.getHeaderForAlias(sessionAlias);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("attachHeaderOrHold builders: " + builders.stream().map(MessageUtils::toJson).collect(Collectors.joining(" | ")));
        }
        if (headerForAlias == null) {
            ByteString extractedHeader = builders.stream()
                    .findFirst()
                    .map(RawMessage.Builder::getBody)
                    .orElseThrow(() -> new IllegalStateException("At least one message must be in the list"));
            HeaderInfo<ByteString> extractedHeaderInfo = headerHolder.setHeaderForAlias(sessionAlias, extractedHeader);
            if (builders.size() < 2) {
                return Collections.emptyList();
            } else {
                return builders.stream()
                        .skip(1)
                        .map(it -> validateAndAppend(headerHolder, extractedHeaderInfo, it, cfg.isValidateContent(), cfg.isValidateOnlyExtraData()))
                        .collect(Collectors.toList());
            }
        } else {
            builders.forEach(it -> validateAndAppend(headerHolder, headerForAlias, it, cfg.isValidateContent(), cfg.isValidateOnlyExtraData()));
            return builders;
        }
    }

    private static RawMessage.Builder validateAndAppend(
            HeaderHolder<ByteString> headerHolder,
            HeaderInfo<ByteString> extractedHeader,
            RawMessage.Builder it,
            boolean validate,
            boolean validateOnlyExtraData
    ) {
        if (validate) {
            headerHolder.validateContentSize(extractedHeader, it.getBody(), validateOnlyExtraData);
        }
        return it.setBody(extractedHeader.getContent().concat(it.getBody()));
    }

    @NotNull
    private static Unit publishMessages(MessageRouter<RawMessageBatch> rawMessageBatchRouter, StreamId streamId, List<? extends RawMessage.Builder> builders) {
        if (builders.isEmpty()) {
            return Unit.INSTANCE;
        }

        try {
            RawMessageBatch.Builder builder = RawMessageBatch.newBuilder();
            for (RawMessage.Builder msg : builders) {
                builder.addMessages(msg);
            }
            rawMessageBatchRouter.sendAll(builder.build());
        } catch (Exception e) {
            LOGGER.error("Cannot publish batch for {}", streamId, e);
        }
        return Unit.INSTANCE;
    }
}