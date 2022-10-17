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

package org.finos.tracdap.common.codec.arrow;

import org.finos.tracdap.common.util.ByteOutputChannel;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;


public class ArrowStreamEncoder extends ArrowEncoder {

    public ArrowStreamEncoder() {

        super();
    }

    @Override
    protected ArrowWriter createWriter(VectorSchemaRoot root) {

        var out = new ByteOutputChannel(bb -> consumer().onNext(bb));
        return new ArrowStreamWriter(root, /* dictionary provider = */ null, out);
    }

}
