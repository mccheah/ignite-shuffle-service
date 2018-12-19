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

import com.google.common.base.Preconditions;
import com.palantir.ignite.SparkShufflePartition;
import com.palantir.ignite.SparkShufflePartitionBlock;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IgniteBlockOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(IgniteBlockOutputStream.class);

    private final IgniteCache<SparkShufflePartitionBlock, byte[]> partitionBlocksDataCache;
    private final SparkShufflePartition partition;
    private final int blockSize;
    private final ByteBuffer buffer;
    private final AtomicLong committedBlockCount;
    private final AtomicLong totalPartitionSize;
    private final AtomicInteger currentBlockSize;

    public IgniteBlockOutputStream(
            IgniteCache<SparkShufflePartitionBlock, byte[]> partitionBlocksDataCache,
            SparkShufflePartition partition,
            int blockSize,
            AtomicLong committedBlockCount,
            AtomicLong totalPartitionSize) {
        this.partitionBlocksDataCache = partitionBlocksDataCache;
        this.partition = partition;
        this.blockSize = blockSize;
        this.buffer = ByteBuffer.allocate(blockSize);
        this.committedBlockCount = committedBlockCount;
        this.totalPartitionSize = totalPartitionSize;
        this.currentBlockSize = new AtomicInteger(0);
    }

    @Override
    public void write(int b) {
        buffer.putInt(b);
        totalPartitionSize.getAndIncrement();
        if (currentBlockSize.incrementAndGet() == blockSize) {
            flush();
        }
    }

    @Override
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void flush() {
        byte[] array = buffer.array();
        Preconditions.checkArgument(array.length == currentBlockSize.get(),
                "Backing array doesn't have the right number of bytes.");
        long currentBlock = committedBlockCount.getAndIncrement();
        SparkShufflePartitionBlock partitionBlock = SparkShufflePartitionBlock.of(partition, currentBlock);
        log.info(
                String.format(
                        "Putting block # %d for partition (app id: %s, shuffle id: %d, map id: %d, reduce id: %d",
                        currentBlock,
                        partitionBlock.partitionId().appId(),
                        partitionBlock.partitionId().shuffleId(),
                        partitionBlock.partitionId().mapId(),
                        partitionBlock.partitionId().partitionId()));
        partitionBlocksDataCache.put(partitionBlock, array);
        buffer.reset();
        currentBlockSize.set(0);
    }

    @Override
    public void close() {
        buffer.limit(currentBlockSize.get());
        flush();
    }
}
