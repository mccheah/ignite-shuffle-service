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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.spark.api.shuffle.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IgniteShufflePartitionWriter implements ShufflePartitionWriter {

    private static final Logger log = LoggerFactory.getLogger(IgniteShufflePartitionWriter.class);

    private final SparkShufflePartition partition;
    private final IgniteDataStreamer<SparkShufflePartitionBlock, byte[]> dataStreamer;
    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final int blockSize;
    private IgniteBlockOutputStream partitionStream;

    public IgniteShufflePartitionWriter(
            SparkShufflePartition partition,
            IgniteDataStreamer<SparkShufflePartitionBlock, byte[]> dataStreamer,
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            int blockSize) {
        this.partition = partition;
        this.dataStreamer = dataStreamer;
        this.dataCache = dataCache;
        this.blockSize = blockSize;
    }

    @Override
    @SuppressWarnings("Slf4jConstantLogMessage")
    public OutputStream toStream() {
        log.info(
                String.format("Asked to open writing partition stream for partition"
                        + " (app id = %s, shuffle id = %d, map id = %d, part id = %d",
                        partition.appId(), partition.shuffleId(), partition.mapId(), partition.partitionId()));
        if (partitionStream != null) {
            throw new IllegalStateException("Opening a stream for the same partition writer more than once.");
        }
        partitionStream = new IgniteBlockOutputStream(dataStreamer, partition, blockSize);
        return partitionStream;
    }

    @Override
    public long getNumBytesWritten() {
        return partitionStream == null ? 0L : partitionStream.getTotalPartitionSize();
    }

    @Override
    public void close() {
        if (partitionStream != null) {
            partitionStream.close();
        }
    }

    public long getNumBlocksInPartition() {
        return partitionStream == null ? 0L : partitionStream.getNumCommittedBlocks();
    }

    public void revertWrites() {
        if (partitionStream != null) {
            Set<SparkShufflePartitionBlock> blocksToRemove = LongStream
                    .range(0, partitionStream.getNumCommittedBlocks())
                    .mapToObj(partitionId -> SparkShufflePartitionBlock.of(partition, partitionId))
                    .collect(Collectors.toSet());
            dataCache.removeAll(blocksToRemove);
        }
    }
}
