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
import org.apache.spark.shuffle.api.ShufflePartitionWriter;

public final class IgniteMapOutputWriter implements ShuffleMapOutputWriter {

    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final IgniteCache<SparkShufflePartition, Long> metadataCache;
    private final String appId;
    private final int shuffleId;
    private final int mapId;
    private final int blockSize;

    public IgniteMapOutputWriter(
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            IgniteCache<SparkShufflePartition, Long> metadataCache,
            String appId,
            int shuffleId,
            int mapId,
            int blockSize) {
        this.dataCache = dataCache;
        this.metadataCache = metadataCache;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.blockSize = blockSize;
    }

    @Override
    public ShufflePartitionWriter newPartitionWriter(int partitionId) {
        return new IgniteShufflePartitionWriter(
                SparkShufflePartition.builder()
                        .appId(appId)
                        .shuffleId(shuffleId)
                        .mapId(mapId)
                        .partitionId(partitionId)
                        .build(),
                dataCache,
                metadataCache,
                blockSize);
    }

    @Override
    public void commitAllPartitions() {

    }

    @Override
    public void abort(Exception exception) {

    }
}
