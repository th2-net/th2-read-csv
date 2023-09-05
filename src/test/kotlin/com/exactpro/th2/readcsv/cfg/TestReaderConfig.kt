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

package com.exactpro.th2.readcsv.cfg

import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestReaderConfig {
    @Test
    fun deserialization() {
        val data = """
            {
              "sourceDirectory": "dir/with/csv/",
              "aliases": {
                "A": {
                  "nameRegexp": "fileA.*\\.log"
                },
                "B": {
                  "nameRegexp": "fileB.*\\.log",
                  "delimiter": ";",
                  "header": [
                    "ColumnA",
                    "ColumnB",
                    "ColumnC"
                  ]
                }
              },
              "common": {
                "staleTimeout": "PT1S",
                "maxBatchSize": 100,
                "maxPublicationDelay": "PT5S",
                "leaveLastFileOpen": false,
                "fixTimestamp": false,
                "maxBatchesPerSecond": -1,
                "disableFileMovementTracking": true
              },
              "pullingInterval": "PT5S",
              "useTransport": true
            }""".trimIndent()

        val cfg = ReaderConfig.MAPPER.readValue<ReaderConfig>(data)

        Assertions.assertEquals(',', cfg.aliases["A"]?.delimiter)
        Assertions.assertEquals(';', cfg.aliases["B"]?.delimiter)
        cfg.pullingInterval
        Assertions.assertEquals(true, cfg.isUseTransport)
    }
}