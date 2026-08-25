/*
 * Tencent is pleased to support the open source community by making dubbo-polaris-java available.
 *
 * Copyright (C) 2021 Tencent. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.polaris.dubbo.example.api.press;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds press-service URL parameters used as Polaris instance metadata.
 *
 * Tune size by editing {@link #DEFAULT_BYTES}, or override at runtime:
 * {@code -Dpress.metadata.bytes=525312} / {@code PRESS_METADATA_BYTES=525312}.
 * Values are split into {@link #CHUNK_BYTES} chunks so a single URL parameter
 * stays below the Java / Dubbo string-literal-style size limits.
 */
public final class PressMetadataPadding {

    public static final String PARAM_PREFIX = "press.meta.padding.";

    /** Default metadata padding size: 512KB + 1KB. */
    public static final int DEFAULT_BYTES = 512 * 1024 + 1024;

    /** Size of one URL parameter value. Must stay below 65535. */
    public static final int CHUNK_BYTES = 48000;

    private PressMetadataPadding() {
    }

    public static int resolveBytes() {
        String value = System.getProperty("press.metadata.bytes");
        if (value == null || value.isEmpty()) {
            value = System.getenv("PRESS_METADATA_BYTES");
        }
        if (value == null || value.isEmpty()) {
            return DEFAULT_BYTES;
        }
        int bytes = Integer.parseInt(value.trim());
        if (bytes < 0) {
            throw new IllegalArgumentException("press.metadata.bytes must be >= 0");
        }
        return bytes;
    }

    /**
     * Generate padding URL parameters for the given total size.
     *
     * @param totalBytes total padding bytes; 0 yields an empty map
     */
    public static Map<String, String> parameters(int totalBytes) {
        if (totalBytes < 0) {
            throw new IllegalArgumentException("totalBytes must be >= 0");
        }
        if (totalBytes == 0) {
            return Collections.emptyMap();
        }
        int chunks = (totalBytes + CHUNK_BYTES - 1) / CHUNK_BYTES;
        Map<String, String> parameters = new LinkedHashMap<>(chunks);
        int remaining = totalBytes;
        for (int i = 0; i < chunks; i++) {
            int size = Math.min(CHUNK_BYTES, remaining);
            parameters.put(String.format("%s%02d", PARAM_PREFIX, i), fill('x', size));
            remaining -= size;
        }
        return parameters;
    }

    public static Map<String, String> parameters() {
        return parameters(resolveBytes());
    }

    private static String fill(char ch, int size) {
        char[] chars = new char[size];
        Arrays.fill(chars, ch);
        return new String(chars);
    }
}
