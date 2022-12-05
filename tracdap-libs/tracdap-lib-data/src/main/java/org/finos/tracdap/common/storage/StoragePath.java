/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


public class StoragePath {

    private final List<String> segments;
    private final boolean absolute;
    private final boolean directory;

    private final boolean isWindows;
    private final Character driveLetter;

    private StoragePath(List<String> segments, boolean absolute, boolean directory) {
        this(segments, absolute, directory, false, null);
    }

    private StoragePath(
            List<String> segments, boolean absolute,  boolean directory,
            boolean isWindows, Character driveLetter) {
        this.segments = segments;
        this.absolute = absolute;
        this.directory = directory;
        this.isWindows = isWindows;
        this.driveLetter = driveLetter;
    }

    public static StoragePath forPath(String literalPath) {

        var segments = literalPath.split("/");
        var absolute = literalPath.startsWith("/");
        var directory = literalPath.endsWith("/");

        if (absolute && segments[0].isEmpty())
            segments = Arrays.copyOfRange(segments, 1, segments.length);

        if (directory && segments[segments.length - 1].isEmpty())
            segments = Arrays.copyOfRange(segments, 0, segments.length - 1);

        return new StoragePath(List.of(segments), absolute, directory);
    }

    public static StoragePath root() {

        return new StoragePath(List.of(), true, true);
    }

    @Override
    public String toString() {

        String path;

        if (isWindows) {

            path = String.join("\\", segments);

            if (absolute)
                path = driveLetter + ":\\" + path;

            if (directory)
                path = path + "\\";
        }
        else {

            path = String.join("/", segments);

            if (absolute)
                path = "/" + path;

            if (directory)
                path = path + "/";

        }

        return path;
    }

    @Override
    public boolean equals(Object obj) {

        if (!(obj instanceof StoragePath))
            return false;

        var path = (StoragePath) obj;

        return
                this.absolute == path.absolute &&
                this.directory == path.directory &&
                this.isWindows == path.isWindows &&
                Objects.equals(this.driveLetter, path.driveLetter) &&
                this.segments.equals(path.segments);
    }

    public boolean isAbsolute() {
        return absolute;
    }

    public boolean isRelative() {
        return !absolute;
    }

    public boolean isDirectory() {
        return directory;
    }

    public boolean startsWith(String segment) {

        return !segments.isEmpty() && segments.get(0).equals(segment);
    }

    public boolean contains(StoragePath path) {

        if (path.isRelative())
            return true;

        if (this.isRelative())
            return false;

        if (isWindows != path.isWindows)
            return false;

        if (!Objects.equals(driveLetter, path.driveLetter))
            return false;

        var thisNormal = this.normalize();
        var thatNormal = path.normalize();

        if (thatNormal.segments.size() < thisNormal.segments.size())
            return false;

        for (int i = 0; i < segments.size(); i++)
            if (!thatNormal.segments.get(i).equals(thisNormal.segments.get(i)))
                return false;

        return true;
    }

    public StoragePath normalize() {

        var normalSegments = new ArrayList<String>();

        for (var segment : segments) {

            if (".".equals(segment))
                continue;

            if ("..".equals(segment)) {

                if (normalSegments.isEmpty()) {

                    normalSegments.add(segment);
                }
                else {

                    var lastSegment = normalSegments.get(normalSegments.size() - 1);

                    if ("..".equals(lastSegment))
                        normalSegments.add(segment);
                    else
                        normalSegments.remove(normalSegments.size() - 1);
                }
            }

            else {

                normalSegments.add(segment);
            }
        }

        if (absolute && !normalSegments.isEmpty() && normalSegments.get(0).equals("..")) {
            throw new StoragePathException("Illegal path [" + this + "]");
        }

        return new StoragePath(normalSegments, absolute, directory, isWindows, driveLetter);
    }

    public StoragePath resolve(StoragePath subPath) {

        if (subPath.isAbsolute())
            return subPath;

        var resolvedSegments = new ArrayList<String>(segments.size() + subPath.segments.size());
        resolvedSegments.addAll(segments);
        resolvedSegments.addAll(subPath.segments);

        return new StoragePath(resolvedSegments, absolute, subPath.directory, isWindows, driveLetter);
    }
}
