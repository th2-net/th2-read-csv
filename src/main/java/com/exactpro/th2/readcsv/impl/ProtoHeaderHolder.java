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
import com.google.protobuf.ByteString;

import java.util.Map;

public class ProtoHeaderHolder extends HeaderHolder<ByteString> {
    private static final ByteString NEW_LINE = ByteString.copyFrom(new byte[]{'\n'});

    public ProtoHeaderHolder(Map<String, CsvFileConfiguration> configurationMap) {
        super(configurationMap);
    }

    protected HeaderInfo<ByteString> formatHeader(String alias, CsvFileConfiguration configuration) {
        ByteString content = ByteString.copyFrom(
                String.join(Character.toString(configuration.getDelimiter()), configuration.getHeader()) + '\n',
                CHARSET
        );
        return new HeaderInfo<>(alias, configuration.getHeader().size(), content);
    }

    @Override
    protected String contentToString(ByteString content) {
        return content.toString(CHARSET);
    }

    @Override
    protected ByteString addNewLine(ByteString content) {
        return content.concat(NEW_LINE);
    }
}
