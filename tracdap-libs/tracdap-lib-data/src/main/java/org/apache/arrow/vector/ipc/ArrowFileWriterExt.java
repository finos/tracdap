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

package org.apache.arrow.vector.ipc;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.HashSet;
import java.util.Set;

public class ArrowFileWriterExt extends ArrowFileWriter {

    // The regular implementation from arrow outputs all dictionaries before the first batch
    // This does not allow for dictionaries to be updated during a streaming operation
    // This implementation delays writing dictionaries until all record batches are output
    // The dictionary provider must be updated with all dictionary values used during the operation

    private final DictionaryProvider dictionaryProvider;
    private final Set<Long> dictionaryIdsUsed;

    public ArrowFileWriterExt(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {

        super(root, provider, out);

        this.dictionaryProvider = provider;
        this.dictionaryIdsUsed = new HashSet<>();
    }

    @Override
    protected void ensureDictionariesWritten(DictionaryProvider provider, Set<Long> dictionaryIdsUsed) {
        // Record Ids to output later
        this.dictionaryIdsUsed.addAll(dictionaryIdsUsed);
    }

    @Override
    protected void endInternal(WriteChannel out) throws IOException {
        // Now output all used dictionaries
        super.ensureDictionariesWritten(dictionaryProvider, dictionaryIdsUsed);
        super.endInternal(out);
    }
}
