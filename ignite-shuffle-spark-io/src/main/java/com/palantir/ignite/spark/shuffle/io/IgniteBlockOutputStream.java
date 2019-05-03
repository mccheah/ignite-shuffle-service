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
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IgniteBlockOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(IgniteBlockOutputStream.class);

    private final IgniteDataStreamer<SparkShufflePartitionBlock, byte[]> dataStreamer;
    private final SparkShufflePartition partition;
    private final int blockSize;

    private long currentBlockIndex;
    private ByteBuffer buffer;
    private long totalPartitionSize;
    private int currentBlockSize;

    public IgniteBlockOutputStream(
            IgniteDataStreamer<SparkShufflePartitionBlock, byte[]> dataStreamer,
            SparkShufflePartition partition,
            int blockSize) {
        this.dataStreamer = dataStreamer;
        this.partition = partition;
        this.blockSize = blockSize;
        this.buffer = ByteBuffer.allocate(blockSize);
        this.totalPartitionSize = 0L;
        this.currentBlockSize = 0;
        this.currentBlockIndex = 0L;
    }

    @Override
    public void write(int b) {
        buffer.put((byte) b);
        totalPartitionSize++;
        currentBlockSize++;
        if (currentBlockSize == blockSize) {
            flush();
        }
    }

    @Override
    @SuppressWarnings("Slf4jConstantLogMessage")
    public void flush() {
        buffer.flip();
        byte[] array = buffer.array();
        byte[] resolvedBlockArray;
        if (currentBlockSize < array.length) {
            resolvedBlockArray = new byte[buffer.remaining()];
            buffer.get(resolvedBlockArray);
        } else {
            resolvedBlockArray = array;
        }
        SparkShufflePartitionBlock partitionBlock = SparkShufflePartitionBlock.of(partition, currentBlockIndex);
        log.info(
                String.format(
                        "Putting block # %d for partition (app id: %s, shuffle id: %d, map id: %d, reduce id: %d",
                        currentBlockIndex,
                        partitionBlock.partitionId().appId(),
                        partitionBlock.partitionId().shuffleId(),
                        partitionBlock.partitionId().mapId(),
                        partitionBlock.partitionId().partitionId()));
        currentBlockIndex += 1;
        dataStreamer.addData(partitionBlock, resolvedBlockArray);
        buffer = ByteBuffer.allocate(blockSize);
        currentBlockSize = 0;
    }

    @Override
    public void close() {
        buffer.limit(currentBlockSize);
        if (currentBlockSize > 0) {
            flush();
        }
        this.buffer = null;
    }

    public long getNumCommittedBlocks() {
        return currentBlockIndex;
    }

    public long getTotalPartitionSize() {
        return totalPartitionSize;
    }
}
