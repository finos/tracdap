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

package org.finos.tracdap.common.data;

import io.netty.util.concurrent.DefaultEventExecutor;
import org.apache.arrow.memory.RootAllocator;
import org.finos.tracdap.common.codec.ICodec;
import org.finos.tracdap.common.codec.csv.CsvCodec;
import org.finos.tracdap.common.data.pipeline.RangeSelector;
import org.finos.tracdap.test.data.DataComparison;
import org.finos.tracdap.test.data.SingleBatchDataSink;
import org.finos.tracdap.test.data.SingleBatchDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.getResultOf;
import static org.finos.tracdap.test.concurrent.ConcurrentTestHelpers.waitFor;
import static org.finos.tracdap.test.data.SampleData.generateBasicData;

public class DataPagingTest {

    private final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

    private final ICodec codec = new CsvCodec();

    @Test
    void roundTrip_basic() {

        var allocator = new RootAllocator();
        var inputData = generateBasicData(allocator, 10000);

        roundTrip_impl(inputData, allocator);
    }

    void roundTrip_impl(ArrowVsrContext inputData, RootAllocator allocator) {

        var ctx = new DataContext(new DefaultEventExecutor(), allocator);

        var dataSrc = new SingleBatchDataSource(inputData);
        var pipeline = DataPipeline.forSource(dataSrc, ctx);
        pipeline.addStage(codec.getEncoder(allocator, Map.of()));;
        pipeline.addStage(codec.getDecoder(inputData.getSchema(), allocator, Map.of()));
        pipeline.addStage(new RangeSelector(1347, 228));

        var dataSink = new SingleBatchDataSink(pipeline, (batch, offset) ->
                DataComparison.compareBatches(inputData, batch, 1347, false));
        pipeline.addSink(dataSink);

        var exec = pipeline.execute();
        waitFor(TEST_TIMEOUT, exec);

        // Ensure errors are reported (pipeline errors or validation failures)
        try {
            getResultOf(exec);
        }
        catch(Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            else
                throw new RuntimeException(e);
        }

        var rtSchema = dataSink.getSchema();
        var rtRowCount = dataSink.getRowCount();

        DataComparison.compareSchemas(inputData.getSchema(), rtSchema);

        Assertions.assertEquals(228, rtRowCount);
        Assertions.assertEquals(1, dataSink.getBatchCount());

        inputData.close();
    }
}
