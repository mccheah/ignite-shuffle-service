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
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IgniteShufflePartitionReader implements ShufflePartitionReader {

    private static final Logger log = LoggerFactory.getLogger(IgniteShufflePartitionReader.class);

    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final IgniteCache<SparkShufflePartition, Long> metadataCache;
    private final String appId;
    private final int shuffleId;
    private final int mapId;

    public IgniteShufflePartitionReader(
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            IgniteCache<SparkShufflePartition, Long> metadataCache,
            String appId,
            int shuffleId,
            int mapId) {
        this.dataCache = dataCache;
        this.metadataCache = metadataCache;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    @SuppressWarnings("Slf4jConstantLogMessage")
    public InputStream fetchPartition(int reduceId) {
        SparkShufflePartition partition = SparkShufflePartition.builder()
                .appId(appId)
                .shuffleId(shuffleId)
                .mapId(mapId)
                .partitionId(reduceId)
                .build();
        log.info(
                String.format("Asked to open reading partition stream for partition"
                                + " (app id = %s, shuffle id = %d, map id = %d, part id = %d",
                        partition.appId(), partition.shuffleId(), partition.mapId(), partition.partitionId()));
        long numBlocks = metadataCache.get(partition);
        Iterator<InputStream> inputStreamIterator = LongStream.range(0, numBlocks)
                .mapToObj(blockNumber -> SparkShufflePartitionBlock.of(partition, blockNumber))
                .map(dataCache::get)
                .map(IgniteShufflePartitionReader::readToInputStream)
                .iterator();
        return new SequenceInputStream(new IteratorEnumeration<>(inputStreamIterator));
    }

    private static InputStream readToInputStream(byte[] bytes) {
        Preconditions.checkNotNull(bytes, "Bytes from cache shouldn't be null.");
        return new ByteArrayInputStream(bytes);
    }

    @SuppressWarnings("JdkObsolete")
    private static final class IteratorEnumeration<T> implements Enumeration<T> {
        private final Iterator<T> iterator;

        IteratorEnumeration(Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasMoreElements() {
            return iterator.hasNext();
        }

        @Override
        public T nextElement() {
            return iterator.next();
        }
    }

}
