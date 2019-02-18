/*
 * Copyright 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;

/**
 * Generates a User Agent string in accordance with https://tools.ietf.org/html/rfc7231#section-5.5.3
 * <p>
 * Example usage:
 * <pre>
 * String userAgent = new UserAgentBuilder()
 *     .append("MyProduct", "1.0", "It's really cool")
 *     .appendJava().appendOs()
 *     .build();
 * </pre>
 */
public class UserAgentBuilder {
    private final List<String> parts = new ArrayList<>();

    /**
     * @param productVersion optional, may be null.
     */
    public UserAgentBuilder append(String productName, String productVersion, String... comments) {
        if (productName.isEmpty()) {
            throw new IllegalArgumentException("Product name must not be empty.");
        }

        final StringBuilder part = new StringBuilder(sanitizeToken(productName));

        if (productVersion != null && !productVersion.isEmpty()) {
            part.append('/').append(sanitizeToken(productVersion));
        }

        final List<String> escapedComments = new ArrayList<>();
        for (String comment : comments) {
            escapedComments.add(escapeComment(sanitizeComment(comment)));
        }
        if (!escapedComments.isEmpty()) {
            part.append(" (").append(String.join("; ", escapedComments)).append(")");
        }

        parts.add(part.toString());
        return this;
    }

    public UserAgentBuilder appendJava() {
        return append("Java", System.getProperty("java.version"), systemProperties("java.vendor", "java.vm.name", "java.vm.version"));
    }

    public UserAgentBuilder appendOs() {
        return append("OS", null, systemProperties("os.name", "os.version", "os.arch"));
    }

    private static String[] systemProperties(String... names) {
        return Arrays.stream(names).map(System::getProperty).toArray(String[]::new);
    }

    public String build() {
        return String.join(" ", parts);
    }

    @Override
    public String toString() {
        return build();
    }

    private static String sanitizeComment(String comment) {
        return sanitize(comment, UserAgentBuilder::isCommentChar, '?');
    }

    private static String sanitizeToken(String token) {
        return sanitize(token, UserAgentBuilder::isTokenChar, '_');
    }

    private static String sanitize(String s, IntPredicate validChar, char invalidCharReplacement) {
        if (s.chars().allMatch(validChar)) {
            return s;
        }

        final StringBuilder sb = new StringBuilder(s.length());
        s.chars().forEach(c -> sb.append(validChar.test(c) ? (char) c : invalidCharReplacement));
        return sb.toString();
    }

    private static boolean isTokenChar(int c) {
        // See https://tools.ietf.org/html/rfc7230#section-3.2.6
        return c >= 'a' && c <= 'z'
                || c >= 'A' && c <= 'Z'
                || c >= '0' && c <= '9'
                || "!#$%&'*+-.^_`|~".indexOf(c) != -1;
    }

    private static boolean isCommentChar(int c) {
        // See https://tools.ietf.org/html/rfc7230#section-3.2.6

        // Allow backslashes and parentheses because we're going to escape them.
        return (c >= 0x20 && c <= 0x7E) || c == '\t';
    }

    private static String escapeComment(String comment) {
        return comment.replace("\\", "\\\\")
                .replace("(", "\\(")
                .replace(")", "\\)");
    }
}
