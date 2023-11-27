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
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage;
import com.exactpro.th2.read.file.common.DirectoryChecker;
import com.exactpro.th2.read.file.common.MovedFileTracker;
import com.exactpro.th2.read.file.common.StreamId;
import com.exactpro.th2.read.file.common.impl.DefaultFileReader;
import com.exactpro.th2.read.file.common.impl.TransportDefaultFileReader;
import com.exactpro.th2.read.file.common.state.impl.InMemoryReaderState;
import com.exactpro.th2.readcsv.cfg.ReaderConfig;
import io.netty.buffer.ByteBuf;
import kotlin.Unit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.netty.buffer.Unpooled.wrappedBuffer;

public class TransportReaderFactory extends AbstractReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportReaderFactory.class);

    public TransportReaderFactory(ReaderConfig configuration, CommonFactory commonFactory) {
        super(configuration, commonFactory);
    }

    @Override
    protected DefaultFileReader.Builder<LineNumberReader, RawMessage.Builder, MessageId.Builder> prepareReaderBuilder(
            MessageRouter<EventBatch> eventBatchRouter,
            DirectoryChecker directoryChecker,
            EventID rootId
    ) {
        var headerHolder = new TransportHeaderHolder(configuration.getAliases());
        var router = commonFactory.getTransportGroupBatchRouter();

        return new TransportDefaultFileReader.Builder<>(
                configuration.getCommon(),
                directoryChecker,
                new TransportCsvContentParser(configuration.getAliases()),
                new MovedFileTracker(configuration.getSourceDirectory()),
                new InMemoryReaderState(),
                streamId -> MessageId.builder(),
                AbstractReaderFactory::createSource
        )
                .onSourceFound((streamId, path) -> clearHeader(headerHolder, streamId))
                .onContentRead((streamId, path, builders) -> transportAttachHeaderOrHold(headerHolder, streamId, builders, configuration))
                .onStreamData((streamId, builders) -> publishTransportMessages(router, streamId, builders, commonFactory.getBoxConfiguration().getBookName()));
    }

    @NotNull
    private static Collection<? extends RawMessage.Builder> transportAttachHeaderOrHold(
            TransportHeaderHolder headerHolder,
            StreamId streamId,
            Collection<? extends RawMessage.Builder> builders,
            ReaderConfig cfg
    ) {
        String sessionAlias = streamId.getSessionAlias();
        HeaderInfo<ByteBuf> headerForAlias = headerHolder.getHeaderForAlias(sessionAlias);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("attachHeaderOrHold builders: " + builders.stream().map(RawMessage.Builder::toString).collect(Collectors.joining(" | ")));
        }
        if (headerForAlias == null) {
            ByteBuf extractedHeader = builders.stream()
                    .findFirst()
                    .map(RawMessage.Builder::getBody)
                    .orElseThrow(() -> new IllegalStateException("At least one message must be in the list"));
            HeaderInfo<ByteBuf> extractedHeaderInfo = headerHolder.setHeaderForAlias(sessionAlias, extractedHeader);
            if (builders.size() < 2) {
                return Collections.emptyList();
            } else {
                return builders.stream()
                        .skip(1)
                        .map(it -> transportValidateAndAppend(headerHolder, extractedHeaderInfo, it, cfg.isValidateContent(), cfg.isValidateOnlyExtraData()))
                        .collect(Collectors.toList());
            }
        } else {
            builders.forEach(it -> transportValidateAndAppend(headerHolder, headerForAlias, it, cfg.isValidateContent(), cfg.isValidateOnlyExtraData()));
            return builders;
        }
    }

    private static RawMessage.Builder transportValidateAndAppend(
            HeaderHolder<ByteBuf> headerHolder,
            HeaderInfo<ByteBuf> extractedHeader,
            RawMessage.Builder builder,
            boolean validate,
            boolean validateOnlyExtraData
    ) {
        if (validate) {
            headerHolder.validateContentSize(extractedHeader, builder.getBody(), validateOnlyExtraData);
        }

        return builder.setBody(wrappedBuffer(extractedHeader.getContent(), builder.getBody()));
    }

    @NotNull
    private static Unit publishTransportMessages(
            MessageRouter<GroupBatch> transportBatchRouter,
            StreamId streamId,
            List<? extends RawMessage.Builder> builders,
            String bookName
    ) {
        if (builders.isEmpty()) {
            return Unit.INSTANCE;
        }

        try {
            // messages are grouped by session aliases
            String sessionGroup = builders.get(0).idBuilder().getSessionAlias();

            List<MessageGroup> groups = new ArrayList<>();
            for (RawMessage.Builder msg : builders) {
                groups.add(new MessageGroup(List.of(msg.build())));
            }
            transportBatchRouter.sendAll(new GroupBatch(bookName, sessionGroup, groups), "transport-group");
        } catch (Exception e) {
            LOGGER.error("Cannot publish batch for {}", streamId, e);
        }
        return Unit.INSTANCE;
    }
}