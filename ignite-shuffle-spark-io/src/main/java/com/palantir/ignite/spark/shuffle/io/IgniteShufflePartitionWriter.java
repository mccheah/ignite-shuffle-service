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
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IgniteShufflePartitionWriter implements ShufflePartitionWriter {

    private static final Logger log = LoggerFactory.getLogger(IgniteShufflePartitionWriter.class);

    private final SparkShufflePartition partition;
    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final IgniteCache<SparkShufflePartition, Long> metadataCache;
    private final AtomicLong blockCount;
    private final AtomicLong totalPartitionSize;
    private final int blockSize;

    public IgniteShufflePartitionWriter(
            SparkShufflePartition partition,
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            IgniteCache<SparkShufflePartition, Long> metadataCache,
            int blockSize) {
        this.partition = partition;
        this.dataCache = dataCache;
        this.metadataCache = metadataCache;
        this.blockSize = blockSize;
        this.blockCount = new AtomicLong(0L);
        this.totalPartitionSize = new AtomicLong(0L);
    }

    @Override
    @SuppressWarnings("Slf4jConstantLogMessage")
    public OutputStream openPartitionStream() {
        log.info(
                String.format("Asked to open writing partition stream for partition"
                        + " (app id = %s, shuffle id = %d, map id = %d, part id = %d",
                        partition.appId(), partition.shuffleId(), partition.mapId(), partition.partitionId()));
        return new IgniteBlockOutputStream(dataCache, partition, blockSize, blockCount, totalPartitionSize);
    }

    @Override
    public long commitAndGetTotalLength() {
        metadataCache.put(partition, blockCount.get());
        return totalPartitionSize.get();
    }

    @Override
    public void abort(Exception failureReason) {
        for (long i = 0; i < blockCount.get(); i++) {
            SparkShufflePartitionBlock block = SparkShufflePartitionBlock.of(partition, i);
            dataCache.remove(block);
        }
    }
}
