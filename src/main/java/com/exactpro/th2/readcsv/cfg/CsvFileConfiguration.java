/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.readcsv.cfg;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CsvFileConfiguration {
    private final Pattern nameRegexp;
    private final char delimiter;

    private List<String> header = Collections.emptyList();

    @JsonCreator
    public CsvFileConfiguration(
            @JsonProperty(value = "nameRegexp", required = true) String regexp,
            @JsonProperty(value = "delimiter", defaultValue = ",") String delimiter
    ) {
        Objects.requireNonNull(regexp, "'nameRegexp' parameter");
        Objects.requireNonNull(delimiter, "'delimiter' parameter");
        if (regexp.isEmpty()) {
            throw new IllegalArgumentException("regexp for file name must not be empty");
        }
        nameRegexp = Pattern.compile(regexp);

        if (delimiter.length() != 1) {
            throw new IllegalArgumentException("The delimiter for CSV should be a single character but got '" + delimiter + "'");
        }
        this.delimiter = delimiter.charAt(0);
    }

    public Pattern getNameRegexp() {
        return nameRegexp;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public List<String> getHeader() {
        return header;
    }

    public void setHeader(List<String> header) {
        this.header = Objects.requireNonNull(header, "'Header' parameter");
    }
}
