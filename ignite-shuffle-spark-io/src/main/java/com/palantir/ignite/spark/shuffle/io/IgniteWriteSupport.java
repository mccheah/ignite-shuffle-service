/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.ignite.spark.shuffle.io;

import com.palantir.ignite.SparkShufflePartition;
import com.palantir.ignite.SparkShufflePartitionBlock;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.spark.api.shuffle.ShuffleMapOutputWriter;
import org.apache.spark.api.shuffle.ShuffleWriteSupport;

public final class IgniteWriteSupport implements ShuffleWriteSupport {

    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final IgniteCache<SparkShufflePartition, Long> metadataCache;
    private final Supplier<IgniteDataStreamer<SparkShufflePartitionBlock, byte[]>> dataStreamerSupplier;
    private final Supplier<IgniteDataStreamer<SparkShufflePartition, Long>> metadataStreamerSupplier;
    private final int blockSize;
    private final String appId;

    public IgniteWriteSupport(
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            IgniteCache<SparkShufflePartition, Long> metadataCache,
            Supplier<IgniteDataStreamer<SparkShufflePartitionBlock, byte[]>> dataStreamerSupplier,
            Supplier<IgniteDataStreamer<SparkShufflePartition, Long>> metadataStreamerSupplier,
            int blockSize,
            String appId) {
        this.dataCache = dataCache;
        this.metadataCache = metadataCache;
        this.dataStreamerSupplier = dataStreamerSupplier;
        this.metadataStreamerSupplier = metadataStreamerSupplier;
        this.blockSize = blockSize;
        this.appId = appId;
    }

    @Override
    public ShuffleMapOutputWriter createMapOutputWriter(
            int shuffleId, int mapId, int numPartitions) {
        return new IgniteMapOutputWriter(
                metadataCache,
                dataCache,
                dataStreamerSupplier.get(),
                metadataStreamerSupplier.get(),
                appId,
                shuffleId,
                mapId,
                blockSize,
                numPartitions);
    }
}
