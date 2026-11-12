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
package org.finos.tracdap.common.storage.local;

import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.storage.IStorageManager;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;


public class LocalStorageRootTest {

    @TempDir
    Path tempDir;

    static NioEventLoopGroup elg;

    @BeforeAll
    static void setupElg() {
        elg = new NioEventLoopGroup(1);
    }

    @AfterAll
    static void shutdownElg() {
        elg.shutdownGracefully();
    }

    @Test
    void missingRoot_created() {

        var rootPath = tempDir.resolve("TENANT_1");
        var storage = localStorage(rootPath);

        Assertions.assertFalse(Files.exists(rootPath));

        storage.start(elg, true);

        Assertions.assertTrue(Files.isDirectory(rootPath));
        storage.stop();
    }

    @Test
    void missingRoot_notCreated() {

        var rootPath = tempDir.resolve("TENANT_1");

        Assertions.assertThrows(EStartup.class, () -> localStorage(rootPath).start(elg));
        Assertions.assertThrows(EStartup.class, () -> localStorage(rootPath).start(elg, false));
        Assertions.assertFalse(Files.exists(rootPath));
    }

    @Test
    void missingParent_notCreated() {

        var rootPath = tempDir.resolve("tenants").resolve("TENANT_1");

        Assertions.assertThrows(EStartup.class, () -> localStorage(rootPath).start(elg, true));
        Assertions.assertFalse(Files.exists(rootPath.getParent()));
    }

    @Test
    void existingRoot_unchanged() throws Exception {

        var rootPath = tempDir.resolve("TENANT_1");
        Files.createDirectory(rootPath);
        Files.writeString(rootPath.resolve("existing_file.txt"), "existing content");

        var storage = localStorage(rootPath);
        storage.start(elg, true);

        Assertions.assertEquals("existing content", Files.readString(rootPath.resolve("existing_file.txt")));
        storage.stop();
    }

    @Test
    void createUnder_nestedRootCreated() throws Exception {

        var base = Files.createDirectory(tempDir.resolve("tenant"));
        var rootPath = base.resolve("TENANT_1").resolve("datasets");
        var storage = localStorage(rootPath, base);

        storage.start(elg, true);

        Assertions.assertTrue(Files.isDirectory(rootPath));

        storage.stop();
    }

    @Test
    void createUnder_missingBase_notCreated() {

        var base = tempDir.resolve("tenant");
        var rootPath = base.resolve("TENANT_1").resolve("datasets");

        Assertions.assertThrows(EStartup.class, () -> localStorage(rootPath, base).start(elg, true));
        Assertions.assertFalse(Files.exists(base));
    }

    @Test
    void createUnder_rootOutsideBase_notCreated() throws Exception {

        var base = Files.createDirectory(tempDir.resolve("tenant"));
        var rootPath = tempDir.resolve("other").resolve("TENANT_1");

        Assertions.assertThrows(EStartup.class, () -> localStorage(rootPath, base).start(elg, true));
        Assertions.assertFalse(Files.exists(rootPath.getParent()));
    }

    @Test
    void createUnder_rootEscapingBase_notCreated() throws Exception {

        var base = Files.createDirectory(tempDir.resolve("tenant"));
        var rootPath = base.resolve("..").resolve("escaped").resolve("TENANT_1");

        Assertions.assertThrows(EStartup.class, () -> localStorage(rootPath, base).start(elg, true));
        Assertions.assertFalse(Files.exists(tempDir.resolve("escaped")));
    }

    @Test
    void createUnder_notAtStartup() throws Exception {

        var base = Files.createDirectory(tempDir.resolve("tenant"));
        var rootPath = base.resolve("TENANT_1").resolve("datasets");

        Assertions.assertThrows(EStartup.class, () -> localStorage(rootPath, base).start(elg, false));
        Assertions.assertFalse(Files.exists(base.resolve("TENANT_1")));
    }

    private LocalFileStorage localStorage(Path rootPath) {

        return localStorage(rootPath, null);
    }

    private LocalFileStorage localStorage(Path rootPath, Path createUnder) {

        var storageProps = new Properties();
        storageProps.put(IStorageManager.PROP_STORAGE_KEY, "TEST_STORAGE");
        storageProps.put(LocalFileStorage.CONFIG_ROOT_PATH, rootPath.toString());

        if (createUnder != null)
            storageProps.put(IStorageManager.PROP_CREATE_UNDER, createUnder.toString());

        return new LocalFileStorage("TEST_STORAGE", storageProps);
    }
}
