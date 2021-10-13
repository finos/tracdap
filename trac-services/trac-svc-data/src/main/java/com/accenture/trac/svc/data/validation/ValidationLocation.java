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

package com.accenture.trac.svc.data.validation;

import java.util.List;
import java.util.Vector;

public class ValidationLocation {

    private final ValidationLocation parent;

    public ValidationLocation(ValidationLocation parent) {
        this.parent = parent;
    }

    public ValidationLocation parent() {
        return parent;
    }

    public List<ValidationLocation> stack() {
        return stack(0);
    }

    private Vector<ValidationLocation> stack(int depth) {

        var stack = (parent == null)
                ? new Vector<ValidationLocation>(depth + 1)
                : parent.stack(depth + 1);

        stack.set(depth, this);

        return stack;
    }
}
