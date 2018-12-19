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
import org.apache.ignite.IgniteCache;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;

public final class IgniteWriteSupport implements ShuffleWriteSupport {

    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final IgniteCache<SparkShufflePartition, Long> metadataCache;
    private final int blockSize;

    public IgniteWriteSupport(
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            IgniteCache<SparkShufflePartition, Long> metadataCache,
            int blockSize) {
        this.dataCache = dataCache;
        this.metadataCache = metadataCache;
        this.blockSize = blockSize;
    }

    @Override
    public ShuffleMapOutputWriter newMapOutputWriter(String appId, int shuffleId, int mapId) {
        return new IgniteMapOutputWriter(
                dataCache,
                metadataCache,
                appId,
                shuffleId,
                mapId,
                blockSize);
    }
}
