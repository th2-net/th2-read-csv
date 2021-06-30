/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import com.exactpro.th2.read.file.common.cfg.CommonFileReaderConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;

public class ReaderConfig {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new KotlinModule())
            .registerModule(new JavaTimeModule());

    @JsonProperty(required = true)
    private Path sourceDirectory;

    private CommonFileReaderConfiguration common = new CommonFileReaderConfiguration();

    @JsonPropertyDescription("The interval between checking for new updates if the reader did not find any updates in the previous attempt")
    private Duration pullingInterval = Duration.ofSeconds(5);

    @JsonPropertyDescription("Mapping between aliases and files parameters to read")
    private Map<String, CsvFileConfiguration> aliases = Collections.emptyMap();

    @JsonPropertyDescription("Checks that the number of columns for read records matches the header size")
    private boolean validateContent = true;

    @JsonPropertyDescription("Disables reporting of errors when the content size is less than the header size. "
            + "Works only if validateContent is enabled")
    private boolean validateOnlyExtraData = false;

    public Path getSourceDirectory() {
        return sourceDirectory;
    }

    public void setSourceDirectory(Path sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    public CommonFileReaderConfiguration getCommon() {
        return common;
    }

    public void setCommon(CommonFileReaderConfiguration common) {
        this.common = common;
    }

    public Duration getPullingInterval() {
        return pullingInterval;
    }

    public void setPullingInterval(Duration pullingInterval) {
        this.pullingInterval = pullingInterval;
    }

    public Map<String, CsvFileConfiguration> getAliases() {
        return aliases;
    }

    public void setAliases(Map<String, CsvFileConfiguration> aliases) {
        this.aliases = aliases;
    }

    public boolean isValidateContent() {
        return validateContent;
    }

    public void setValidateContent(boolean validateContent) {
        this.validateContent = validateContent;
    }

    public boolean isValidateOnlyExtraData() {
        return validateOnlyExtraData;
    }

    public void setValidateOnlyExtraData(boolean validateOnlyExtraData) {
        this.validateOnlyExtraData = validateOnlyExtraData;
    }
}

