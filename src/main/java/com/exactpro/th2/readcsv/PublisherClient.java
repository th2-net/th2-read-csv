/**
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.readcsv;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;

import net.logstash.logback.argument.StructuredArguments;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;

public class PublisherClient implements AutoCloseable {

    private static final int LINES_LIMIT = 100;
    private static final int SIZE_LIMIT = 100000000;
    private final Logger logger = LoggerFactory.getLogger(PublisherClient.class);

	private String sessionAlias = "";
	private long index = firstSequence();
	private List<String> listOfLines = new ArrayList<String>();
	private long size = 0;
	private long lastPublishTs = Clock.systemDefaultZone().instant().getEpochSecond();
    private final MessageRouter<RawMessageBatch> batchMessageRouter;

    private String header =  "";

    private static long firstSequence() {
        Instant now = Instant.now();
        return TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    }

    public PublisherClient(String sessionAlias, MessageRouter<RawMessageBatch> batchMessageRouter) {
        this(sessionAlias, batchMessageRouter, LINES_LIMIT, SIZE_LIMIT);
    }
    
    public PublisherClient(String sessionAlias, MessageRouter<RawMessageBatch> batchMessageRouter, int linesLimit, int charactersLimit) {
        this.sessionAlias = Objects.requireNonNull(sessionAlias, "'Session alias' parameter");

        this.batchMessageRouter = Objects.requireNonNull(batchMessageRouter, "'Batch message router' parameter");        
	}

    public void setCsvHeader(String csvHeader) {
        this.header = Objects.requireNonNull(csvHeader, "'CsvHeader' parameter");
    }
    
	private void publish() throws IOException {
		RawMessageBatch.Builder builder = RawMessageBatch.newBuilder();
		
		boolean isHeader = true;
		
		for (String str: listOfLines) {
			
			if (isHeader) {
				buildMessage(builder, header, isHeader);
				isHeader = false;
			}
			
			buildMessage(builder, str, isHeader);
		}
		
		listOfLines.clear();
		
        RawMessageBatch batch = builder.build();

        if (batch.getMessagesCount() > 0) {
            batchMessageRouter.sendAll(batch, QueueAttribute.PUBLISH.toString(), QueueAttribute.RAW.toString());

            logger.trace("Raw batch published: {}", JsonFormat.printer().omittingInsignificantWhitespace().print(batch));
        } else {
            logger.trace("Skip publishing empty batch");
        }											
	}
	
	private void buildMessage(RawMessageBatch.Builder builder, String str, boolean isHeader) {
		RawMessage.Builder msgBuilder = builder.addMessagesBuilder();
		
		ByteString body = ByteString.copyFrom(str.getBytes());

		msgBuilder.setBody(body);

		RawMessageMetadata.Builder metaData = RawMessageMetadata.newBuilder();
		
		Timestamp.Builder ts = Timestamp.newBuilder();
		
		Clock clock = Clock.systemDefaultZone();
		Instant instant = clock.instant();
		
		ts.setSeconds(instant.getEpochSecond());
		ts.setNanos(instant.getNano());
		
		if (isHeader) {
			metaData.putProperties("message.type", "header");
		}
		
		metaData.setTimestamp(ts);

		MessageID.Builder messageId = MessageID.newBuilder();
		
		ConnectionID.Builder connId = ConnectionID.newBuilder();
		
		connId.setSessionAlias(sessionAlias);
		
		messageId.setConnectionId(connId);
		messageId.setSequence(++index);
		messageId.setDirection(Direction.FIRST);
		
		metaData.setId(messageId);
		
		msgBuilder.setMetadata(metaData);
	}
	
	public boolean publish(String line) throws IOException {
		
		if (line.isBlank()) {
			return false;
		}
		
        int dataSize = line.getBytes().length;
        if (dataSize >= SIZE_LIMIT) {
            throw new IllegalArgumentException("The input line can contain only " + SIZE_LIMIT + " bytes but has " + dataSize);
        }

        boolean published = false;
        if (size + dataSize >= SIZE_LIMIT) {
            resetAndPublish();
            published = true;
        }
        size += line.length();
		
		listOfLines.add(line);
		
		if (	(listOfLines.size() >= LINES_LIMIT) ||
				(Clock.systemDefaultZone().instant().getEpochSecond() - lastPublishTs > 2)) {

            resetAndPublish();
            return true;
        }
        return published;
	}

    private void resetAndPublish() throws IOException {
        lastPublishTs = Clock.systemDefaultZone().instant().getEpochSecond();
        size = 0;

        publish();
    }
	
	@Override
    public void close() throws IOException, TimeoutException {
		
		if (!listOfLines.isEmpty()) {
			publish();
		}
		
		logger.info("Disconnecting from RabbitMQ");
	}
}
