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

import com.google.common.collect.Maps;
import com.palantir.ignite.SparkShufflePartition;
import com.palantir.ignite.SparkShufflePartitionBlock;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.shuffle.MapShuffleLocations;
import org.apache.spark.api.shuffle.ShuffleMapOutputWriter;
import org.apache.spark.api.shuffle.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IgniteMapOutputWriter implements ShuffleMapOutputWriter {

    private static final Logger LOG = LoggerFactory.getLogger(IgniteMapOutputWriter.class);

    private final IgniteCache<SparkShufflePartition, Long> metadataCache;
    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final IgniteDataStreamer<SparkShufflePartitionBlock, byte[]> dataStreamer;
    private final IgniteDataStreamer<SparkShufflePartition, Long> metadataStreamer;
    private final Map<SparkShufflePartition, IgniteShufflePartitionWriter> openedWriters;
    private final String appId;
    private final int shuffleId;
    private final int mapId;
    private final int blockSize;
    private final int numPartitions;

    public IgniteMapOutputWriter(
            IgniteCache<SparkShufflePartition, Long> metadataCache,
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            IgniteDataStreamer<SparkShufflePartitionBlock, byte[]> dataStreamer,
            IgniteDataStreamer<SparkShufflePartition, Long> metadataStreamer,
            String appId,
            int shuffleId,
            int mapId,
            int blockSize,
            int numPartitions) {
        this.metadataCache = metadataCache;
        this.dataCache = dataCache;
        this.dataStreamer = dataStreamer;
        this.metadataStreamer = metadataStreamer;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.blockSize = blockSize;
        this.numPartitions = numPartitions;
        this.openedWriters = Maps.newHashMapWithExpectedSize(numPartitions);
    }

    @Override
    public ShufflePartitionWriter getPartitionWriter(int partitionId) {
        SparkShufflePartition partition = SparkShufflePartition.builder()
                .shuffleId(shuffleId)
                .mapId(mapId)
                .appId(appId)
                .partitionId(partitionId)
                .build();
        IgniteShufflePartitionWriter partWriter = new IgniteShufflePartitionWriter(
                SparkShufflePartition.builder()
                        .appId(appId)
                        .shuffleId(shuffleId)
                        .mapId(mapId)
                        .partitionId(partitionId)
                        .build(),
                dataStreamer,
                dataCache,
                blockSize);
        openedWriters.put(partition, partWriter);
        return partWriter;
    }

    @Override
    public Optional<MapShuffleLocations> commitAllPartitions() {
        dataStreamer.close();
        Map<SparkShufflePartition, Long> numBlocksPerPartition = Maps.newHashMap();
        numBlocksPerPartition.putAll(Maps.transformValues(
                openedWriters,
                IgniteShufflePartitionWriter::getNumBlocksInPartition));
        numBlocksPerPartition.putAll(IntStream.range(0, numPartitions)
                .mapToObj(partitionId ->
                        SparkShufflePartition.builder()
                                .appId(appId)
                                .shuffleId(shuffleId)
                                .mapId(mapId)
                                .partitionId(partitionId)
                                .build())
                .filter(part -> !numBlocksPerPartition.containsKey(part))
                .collect(Collectors.toMap(Function.identity(), ignored -> 0L)));
        metadataStreamer.addData(numBlocksPerPartition);
        metadataStreamer.close();
        openedWriters.clear();
        return Optional.empty();
    }

    @Override
    public void abort(Throwable error) {
        dataStreamer.close();
        openedWriters.values().forEach(writer -> {
            try {
                writer.revertWrites();
            } catch (Exception e) {
                LOG.warn("Failed to revert some partition writes for a partition.", e);
            }
        });
        openedWriters.clear();
    }
}
