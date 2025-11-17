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

package org.finos.tracdap.common.storage;

import org.finos.tracdap.metadata.*;

public class LayoutItem {

    private final TagHeader header;
    private final FileDefinition file;
    private final DataDefinition data;
    private final SchemaDefinition schema;
    private final StorageDefinition storage;

    private final PartKey part;
    private final int snap;
    private final int delta;

    private final String mimeType;
    private final String extension;


    public static LayoutItem forFile(TagHeader header, FileDefinition file) {
        return new LayoutItem(header, file);
    }

    private LayoutItem(TagHeader header, FileDefinition file) {

        this.header = header;

        this.file = file;
        this.data = null;
        this.schema = null;
        this.storage = null;

        this.part = null;
        this.snap = 0;
        this.delta = 0;

        this.mimeType = file.getMimeType();
        this.extension = file.getExtension();
    }

    public static LayoutItem forPriorFile(FileDefinition file, StorageDefinition storage) {
        return new LayoutItem(file, storage);
    }

    private LayoutItem(FileDefinition file, StorageDefinition storage) {

        this.header = null;

        this.file = file;
        this.data = null;
        this.schema = null;
        this.storage = storage;

        this.part = null;
        this.snap = 0;
        this.delta = 0;

        this.mimeType = null;
        this.extension = null;
    }

    public static LayoutItem forData(
            TagHeader header, DataDefinition data, SchemaDefinition schema,
            PartKey part, int snap, int delta,
            String mimeType, String extension) {

        return new LayoutItem(header, data, schema, part, snap, delta, mimeType, extension);
    }

    private LayoutItem(
            TagHeader header, DataDefinition data, SchemaDefinition schema,
            PartKey part, int snap, int delta,
            String mimeType, String extension) {

        this.header = header;

        this.file = null;
        this.data = data;
        this.schema = schema;
        this.storage = null;

        this.part = part;
        this.snap = snap;
        this.delta = delta;

        this.mimeType = mimeType;
        this.extension = extension;
    }

    public static LayoutItem forPriorData(DataDefinition data, SchemaDefinition schema, StorageDefinition storage) {
        return new LayoutItem(data, schema, storage);
    }

    private LayoutItem(DataDefinition data, SchemaDefinition schema, StorageDefinition storage) {

        this.header = null;

        this.file = null;
        this.data = data;
        this.schema = schema;
        this.storage = storage;

        this.part = null;
        this.snap = 0;
        this.delta = 0;

        this.mimeType = null;
        this.extension = null;
    }

    public TagHeader header() {
        return header;
    }

    public FileDefinition file() {
        return file;
    }

    public DataDefinition data() {
        return data;
    }

    public SchemaDefinition schema() {
        return schema;
    }

    public StorageDefinition storage() {
        return storage;
    }

    public PartKey part() {
        return part;
    }

    public int snap() {
        return snap;
    }

    public int delta() {
        return delta;
    }

    public String mimeType() {
        return mimeType;
    }

    public String extension() {
        return extension;
    }
}
