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

import java.io.File;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ReaderConfig {
    public static final int NO_LIMIT = -1;

    @JsonProperty(value = "csv-file", required = true)
    private File csvFile;
    
    @JsonProperty(value = "csv-header", required = true)
    private String csvHeader;

    
    @JsonProperty("max-batches-per-second")
    private int maxBatchesPerSecond = NO_LIMIT;

    public File getCsvFile() {
        return csvFile;
    }

    public void setCsvFile(File logFile) {
        this.csvFile = logFile;
    }

    public String getCsvHeader() {
        return csvHeader;
    }

    public void setCsvHeader(String csvHeader) {
        this.csvHeader = csvHeader;
    }
    
    public int getMaxBatchesPerSecond() {
        return maxBatchesPerSecond;
    }

    public void setMaxBatchesPerSecond(int maxBatchesPerSecond) {
        this.maxBatchesPerSecond = maxBatchesPerSecond;
    }


}

