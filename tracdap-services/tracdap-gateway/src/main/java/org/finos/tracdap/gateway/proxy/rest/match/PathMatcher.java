/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.gateway.proxy.rest.match;

import java.util.List;


public class PathMatcher implements Matcher<List<String>> {

    private final List<Matcher<String>> segmentMatchers;
    private final Matcher<List<String>> multiSegmentMatcher;

    public PathMatcher(List<Matcher<String>> segmentMatchers, Matcher<List<String>> multiSegmentMatcher) {
        this.segmentMatchers = segmentMatchers;
        this.multiSegmentMatcher = multiSegmentMatcher;
    }

    public PathMatcher(List<Matcher<String>> segmentMatchers) {
        this.segmentMatchers = segmentMatchers;
        this.multiSegmentMatcher = null;
    }

    @Override
    public boolean matches(List<String> pathSegments) {

        if (pathSegments.size() < segmentMatchers.size())
            return false;

        if (pathSegments.size() > segmentMatchers.size() && multiSegmentMatcher == null) {
            return false;
        }

        for (int i = 0; i < segmentMatchers.size(); i++) {

            var matcher = segmentMatchers.get(i);
            var segment = pathSegments.get(i);

            if (!matcher.matches(segment))
                return false;
        }

        if (multiSegmentMatcher == null)
            return true;

        var remainingSegments = pathSegments.subList(segmentMatchers.size(), pathSegments.size());
        return multiSegmentMatcher.matches(remainingSegments);
    }

    /**
     * Orders two path templates so the more specific one comes first: at the first segment
     * where one template has a literal and the other does not, the literal wins. Otherwise the
     * template with more fixed segments wins.
     */
    public int compareSpecificity(PathMatcher other) {

        var commonLength = Math.min(segmentMatchers.size(), other.segmentMatchers.size());

        for (int i = 0; i < commonLength; i++) {

            var literal = segmentMatchers.get(i) instanceof LiteralMatcher;
            var otherLiteral = other.segmentMatchers.get(i) instanceof LiteralMatcher;

            if (literal != otherLiteral)
                return literal ? -1 : 1;
        }

        return Integer.compare(other.segmentMatchers.size(), segmentMatchers.size());
    }
}
