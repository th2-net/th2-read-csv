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

package com.exactpro.th2.readcsv.impl

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.read.file.common.StreamId
import com.exactpro.th2.readcsv.cfg.CsvFileConfiguration
import com.exactpro.th2.readcsv.exception.MalformedCsvException
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.io.BufferedReader

class TransportCsvContentParserTest {
    private val parser = TransportCsvContentParser(
        mapOf(
            "test" to CsvFileConfiguration(".*", ",")
        )
    )
    private val streamId = StreamId("test")

    @Test
    fun `can parse valid csv`() {
        validCsv().use {
            it.mark(1024)
            val canParse = parser.canParse(streamId, it, true)
            assertTrue(canParse) {
                "Parse should be able to parse the valid csv"
            }
            it.reset()

            val parsed: Collection<RawMessage.Builder> = parser.parse(streamId, it)
            assertEquals(1, parsed.size)
            assertEquals("This,is,valid,\"multiline\nCSV file\"", parsed.first().body.toString(Charsets.UTF_8))
        }
    }

    @Test
    fun `does not throw malformed exception if file can be changed`() {
        malformedCsv().use {
            val canParse = parser.canParse(streamId, it, false)
            assertFalse(canParse) {
                "Parse should not be able to parse the malformed csv"
            }
        }
    }

    @Test
    fun `throws malformed exception if file can not be changed`() {
        malformedCsv().use {
            assertThrows(MalformedCsvException::class.java) {
                parser.canParse(streamId, it, true)
            }
        }
    }

    private fun validCsv(): BufferedReader = readerForResource("valid.csv")

    private fun malformedCsv(): BufferedReader = readerForResource("malformed.csv")

    private fun readerForResource(resourceName: String): BufferedReader {
        val resourceAsStream = TransportCsvContentParserTest::class.java.classLoader.getResourceAsStream(resourceName)
        return requireNotNull(resourceAsStream) {
            "Unknown resource: $resourceName"
        }.bufferedReader()
    }
}