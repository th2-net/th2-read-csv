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

import com.exactpro.th2.readcsv.cfg.CsvFileConfiguration;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Map;

public class TransportHeaderHolder extends HeaderHolder<ByteBuf> {
    private static final byte NEW_LINE = '\n';

    public TransportHeaderHolder(Map<String, CsvFileConfiguration> configurationMap) {
        super(configurationMap);
    }

    protected HeaderInfo<ByteBuf> formatHeader(String alias, CsvFileConfiguration configuration) {
        String headerString = String.join(Character.toString(configuration.getDelimiter()), configuration.getHeader()) + '\n';
        ByteBuf content = Unpooled.wrappedBuffer(headerString.getBytes(CHARSET));
        return new HeaderInfo<>(alias, configuration.getHeader().size(), content);
    }

    @Override
    protected String contentToString(ByteBuf content) {
        return null;
    }

    @Override
    protected ByteBuf addNewLine(ByteBuf content) {
        return content.writeByte(NEW_LINE);
    }
}