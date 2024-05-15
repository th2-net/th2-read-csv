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

import java.util.Objects;

public class HeaderInfo<CONTENT_TYPE> {
    private final String alias;
    private final int size;
    private final CONTENT_TYPE content;

    public HeaderInfo(String alias, int size, CONTENT_TYPE content) {
        this.alias = Objects.requireNonNull(alias, "'Alias' parameter");
        this.size = size;
        this.content = Objects.requireNonNull(content, "'Content' parameter");
    }

    public String getAlias() {
        return alias;
    }

    public int getSize() {
        return size;
    }

    public CONTENT_TYPE getContent() {
        return content;
    }
}
