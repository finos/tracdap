/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.data.core;

import java.time.Instant;
import java.util.Optional;

public class FileStat {

    public FileStat(
            FileType fileType, long size,
            Instant ctime, Instant mtime, Instant atime,
            Integer uid, Integer gid, Integer mode) {

        this.fileType = fileType;
        this.size = size;
        this.ctime = ctime;
        this.mtime = mtime;
        this.atime = atime;
        this.uid = Optional.ofNullable(uid);
        this.gid = Optional.ofNullable(gid);
        this.mode = Optional.ofNullable(mode);
    }

    public final FileType fileType;
    public final long size;

    public final Instant ctime;
    public final Instant mtime;
    public final Instant atime;

    public final Optional<Integer> uid;
    public final Optional<Integer> gid;
    public final Optional<Integer> mode;
}
