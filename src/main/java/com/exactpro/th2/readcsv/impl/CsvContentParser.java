/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.read.file.common.ContentParser;
import com.exactpro.th2.read.file.common.StreamId;
import com.exactpro.th2.read.file.common.recovery.RecoverableException;
import com.exactpro.th2.readcsv.cfg.CsvFileConfiguration;
import com.exactpro.th2.readcsv.exception.MalformedCsvException;
import com.google.protobuf.ByteString;
import com.opencsv.ConfigurableCsvParser;
import org.jetbrains.annotations.NotNull;

public class CsvContentParser implements ContentParser<BufferedReader> {
    private static final byte[] NEW_LINE_BYTES = {'\n'};
    private final Map<String, CsvFileConfiguration> configurationMap;

    public CsvContentParser(Map<String, CsvFileConfiguration> configurationMap) {
        this.configurationMap = Objects.requireNonNull(configurationMap, "'Configuration map' parameter");
        if (configurationMap.isEmpty()) {
            throw new IllegalArgumentException("At least one alias must me specified");
        }
    }

    @Override
    public boolean canParse(@NotNull StreamId streamId, BufferedReader bufferedReader, boolean considerNoFutureUpdates) {
        List<String> lines = tryReadRecord(getConfiguration(streamId), streamId, bufferedReader, considerNoFutureUpdates);
        return !lines.isEmpty();
    }

    @NotNull
    @Override
    public Collection<RawMessage.Builder> parse(@NotNull StreamId streamId, BufferedReader bufferedReader) {
        List<String> lines = tryReadRecord(getConfiguration(streamId), streamId, bufferedReader, true);
        return List.of(RawMessage.newBuilder().setBody(joinLines(lines)));
    }

    private ByteString joinLines(List<String> lines) {
        Charset charset = StandardCharsets.UTF_8;
        List<byte[]> stringBytes = lines.stream()
                .map(it -> it.getBytes(charset))
                .collect(Collectors.toList());
        int totalSize = stringBytes.stream().mapToInt(it -> it.length).sum() + (stringBytes.size() - 1);
        byte[] bytes = new byte[totalSize];
        int pos = 0;
        int size = stringBytes.size();
        for (int i = 0; i < size; i++) {
            byte[] src = stringBytes.get(i);
            System.arraycopy(src, 0, bytes, pos, src.length);
            pos += src.length;
            if (i != size - 1) {
                System.arraycopy(NEW_LINE_BYTES, 0, bytes, pos, NEW_LINE_BYTES.length);
                pos += NEW_LINE_BYTES.length;
            }
        }
        return ByteString.copyFrom(bytes);
    }

    @NotNull
    private CsvFileConfiguration getConfiguration(@NotNull StreamId streamId) {
        return Objects.requireNonNull(configurationMap.get(streamId.getSessionAlias()),
                () -> "Cannot find configuration for streamId: " + streamId);
    }

    private List<String> tryReadRecord(CsvFileConfiguration cfg, StreamId streamId, BufferedReader reader, boolean considerNoFutureUpdates) {
        var parser = new ConfigurableCsvParser(cfg.getDelimiter());
        String nextLine;
        var lines = new ArrayList<String>();
        try {
            do {
                nextLine = readLine(reader, considerNoFutureUpdates);
                if (nextLine != null) {
                    parser.parseLineMulti(nextLine);
                    lines.add(nextLine);
                }
            } while (parser.isPending() && nextLine != null);
        } catch (IOException ex) {
            throw new RuntimeException("Cannot read the record for " + streamId, ex);
        }

        if (parser.isPending()) {
            if (considerNoFutureUpdates) {
                throw new MalformedCsvException("The file for stream ID " + streamId + " contains malformed CSV line. Read lines: "
                        + String.join(Character.toString(cfg.getDelimiter()), lines));
            }
            return Collections.emptyList();
        } else {
            return lines;
        }
    }

    private String readLine(BufferedReader reader, boolean considerNoFutureUpdates) throws IOException {
        String nextLine;
        try {
            nextLine = reader.readLine();
        } catch (MalformedInputException ex) {
            if (considerNoFutureUpdates) {
                // because there won't be more bytes. so the file is corrupted
                throw new RuntimeException(ex);
            }
            throw new RecoverableException(ex);
        }
        if (reader.ready()) {
            return nextLine;
        }
        if (considerNoFutureUpdates) {
            return nextLine;
        }
        return null;
    }
}
