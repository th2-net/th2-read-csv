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

import com.exactpro.th2.readcsv.cfg.CsvFileConfiguration
import io.netty.buffer.Unpooled.wrappedBuffer
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class TransportHeaderHolderTest {
    private val holder = TransportHeaderHolder(
        mapOf(
            "A" to CsvFileConfiguration(".*", ","),
            "B" to CsvFileConfiguration(".*", ",").apply {
                header = listOf("A", "B", "C")
            },
        )
    )

    @Test
    fun `holds the header from cfg`() {
        val headerInfo = holder.getHeaderForAlias("B")
        assertNotNull(headerInfo) { "Cannot find info for alias B" }
        assertEquals(3, headerInfo!!.size) {
            "Unexpected size for header: " + headerInfo.content.toString(Charsets.UTF_8)
        }
    }

    @Test
    fun `does not remove the header from cfg`() {
        holder.clearHeaderForAlias("B")

        val headerInfo = holder.getHeaderForAlias("B")
        assertNotNull(headerInfo) { "Cannot find info for alias B" }
    }

    @Test
    fun `holds header from file`() {
        holder.setHeaderForAlias("A", wrappedBuffer("Header,with,\"Multi\nline\"".toByteArray()))

        val headerInfo = holder.getHeaderForAlias("A")
        assertNotNull(headerInfo) { "Cannot find info for alias A" }
        assertEquals(3, headerInfo!!.size) {
            "Unexpected size for header: " + headerInfo.content.toString(Charsets.UTF_8)
        }
        assertEquals("Header,with,\"Multi\nline\"\n", headerInfo.content.toString(Charsets.UTF_8))
    }

    @Test
    fun `clears header from file`() {
        holder.setHeaderForAlias("A", wrappedBuffer("Header,with,\"Multi\nline\"".toByteArray()))
        holder.clearHeaderForAlias("A")

        val headerInfo = holder.getHeaderForAlias("A")
        assertNull(headerInfo) { "Header info for A was not cleared: ${headerInfo!!.content.toString(Charsets.UTF_8)}" }
    }

    @Test
    fun `reports error if content size does not match header`() {
        val headerForAlias = holder.getHeaderForAlias("B")
        assertThrows(IllegalStateException::class.java) {
            holder.validateContentSize(headerForAlias, wrappedBuffer("1,2,3,4".toByteArray()), false)
        }
    }

    @Test
    fun `does not report error if content size is less than header and reporting disabled`() {
        val headerForAlias = holder.getHeaderForAlias("B")
        assertDoesNotThrow {
            holder.validateContentSize(headerForAlias, wrappedBuffer("1,2".toByteArray()), true)
        }
    }

    @Test
    fun `does not report error if content size matches the header`() {
        val headerForAlias = holder.getHeaderForAlias("B")
        assertDoesNotThrow {
            holder.validateContentSize(headerForAlias, wrappedBuffer("1,2,3".toByteArray()), false)
        }
    }
}