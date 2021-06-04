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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.exactpro.th2.readcsv.cfg.CsvFileConfiguration;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableMap;

public class HeaderHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeaderHolder.class);
    private static final ByteString NEW_LINE = ByteString.copyFrom(new byte[]{'\n'});
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private final Map<String, ByteString> encodedHeadersByAlias = new ConcurrentHashMap<>();
    private final Map<String, ByteString> constantHeadersByAlias;

    public HeaderHolder(Map<String, CsvFileConfiguration> configurationMap) {
        constantHeadersByAlias = configurationMap.entrySet().stream()
                .filter(not(it -> it.getValue().getHeader().isEmpty()))
                .collect(toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> formatHeader(entry.getValue()))
                );
    }

    private static ByteString formatHeader(CsvFileConfiguration configuration) {
        return ByteString.copyFrom(
                String.join(Character.toString(configuration.getDelimiter()), configuration.getHeader()) + '\n',
                CHARSET
        );
    }

    @Nullable
    public ByteString getHeaderForAlias(String alias) {
        return constantHeadersByAlias.getOrDefault(alias, encodedHeadersByAlias.get(alias));
    }

    public void setHeaderForAlias(String alias, ByteString encodedHeader) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Set header {} for alias: {}", encodedHeader.toString(CHARSET), alias);
        }
        encodedHeadersByAlias.put(alias, encodedHeader.concat(NEW_LINE));
    }

    public void clearHeaderForAlias(String alias) {
        ByteString removed = encodedHeadersByAlias.remove(alias);
        if (removed != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Header for alias {} removed. Header: {}", alias, removed.toString(CHARSET));
            }
        }
    }
}
