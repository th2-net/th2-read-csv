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

package com.exactpro.th2.readcsv.impl;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.exactpro.th2.readcsv.cfg.CsvFileConfiguration;
import com.opencsv.ConfigurableCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableMap;

public abstract class HeaderHolder<CONTENT_TYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderHolder.class);
    protected static final Charset CHARSET = StandardCharsets.UTF_8;

    private final Map<String, HeaderInfo<CONTENT_TYPE>> encodedHeadersByAlias = new ConcurrentHashMap<>();
    private final Map<String, HeaderInfo<CONTENT_TYPE>> constantHeadersByAlias;
    private final Map<String, CsvFileConfiguration> cfgByAlias;

    public HeaderHolder(Map<String, CsvFileConfiguration> configurationMap) {
        cfgByAlias = Objects.requireNonNull(configurationMap, "'Configuration map' parameter");
        constantHeadersByAlias = configurationMap.entrySet().stream()
                .filter(not(it -> it.getValue().getHeader().isEmpty()))
                .collect(toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> formatHeader(entry.getKey(), entry.getValue()))
                );
    }

    public void validateContentSize(HeaderInfo<CONTENT_TYPE> headerInfo, CONTENT_TYPE content, boolean reportOnlyExtraData) {
        CsvFileConfiguration cfg = Objects.requireNonNull(cfgByAlias.get(headerInfo.getAlias()),
                () -> "Unknown alias: " + headerInfo.getAlias());
        int contentSize = extractColumnsNumber(contentToString(content), cfg.getDelimiter());
        if (!reportOnlyExtraData && contentSize < headerInfo.getSize()) {
            throw new IllegalStateException("The number of columns in the content is less then the header size. Header size: "
                    + headerInfo.getSize() + "; Content size: " + contentSize + "; Content: " + contentToString(content));
        }

        if (contentSize > headerInfo.getSize()) {
            throw new IllegalStateException("The number of columns in the content is greater then the header size. Header size: "
                    + headerInfo.getSize() + "; Content: " + contentSize + "; Content: " + contentToString(content));
        }
    }

    protected abstract String contentToString(CONTENT_TYPE content);
    protected abstract HeaderInfo<CONTENT_TYPE> formatHeader(String alias, CsvFileConfiguration configuration);
    protected abstract CONTENT_TYPE addNewLine(CONTENT_TYPE content);

    @Nullable
    public HeaderInfo<CONTENT_TYPE> getHeaderForAlias(String alias) {
        return constantHeadersByAlias.getOrDefault(alias, encodedHeadersByAlias.get(alias));
    }

    public HeaderInfo<CONTENT_TYPE> setHeaderForAlias(String alias, CONTENT_TYPE encodedHeader) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Set header {} for alias: {}", contentToString(encodedHeader), alias);
        }

        CONTENT_TYPE content = addNewLine(encodedHeader);
        CsvFileConfiguration cfg = Objects.requireNonNull(cfgByAlias.get(alias), () -> "Unexpected alias: " + alias);
        HeaderInfo<CONTENT_TYPE> headerInfo = new HeaderInfo<>(alias, extractColumnsNumber(contentToString(content), cfg.getDelimiter()), content);
        encodedHeadersByAlias.put(alias, headerInfo);
        return headerInfo;
    }

    public void clearHeaderForAlias(String alias) {
        HeaderInfo<CONTENT_TYPE> removed = encodedHeadersByAlias.remove(alias);
        if (removed != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Header for alias {} removed. Header: {}", alias, contentToString(removed.getContent()));
            }
        }
    }

    private static int extractColumnsNumber(String headerString, char delimiter) {
        try {
            String[] strings = new ConfigurableCsvParser(delimiter).parseLine(headerString);
            if (strings.length == 0) {
                throw new IllegalStateException("Extracted header size is 0: " + headerString);
            }
            return strings.length;
        } catch (IOException e) {
            throw new RuntimeException("Cannot extract header size from: " + headerString + "; Delimiter: " + delimiter, e);
        }
    }
}

