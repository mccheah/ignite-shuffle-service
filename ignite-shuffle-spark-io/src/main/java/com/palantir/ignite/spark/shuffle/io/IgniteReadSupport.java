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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.ignite.SparkShufflePartition;
import com.palantir.ignite.SparkShufflePartitionBlock;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Deque;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.spark.api.shuffle.ShuffleBlockInfo;
import org.apache.spark.api.shuffle.ShuffleReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IgniteReadSupport implements ShuffleReadSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IgniteReadSupport.class);

    private final IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private final IgniteCache<SparkShufflePartition, Long> metadataCache;
    private final ListeningExecutorService blockFetcherExecutor;
    private final String appId;
    private final int numBlocksPerBatch;
    private final int maxBatchesInFlight;

    public IgniteReadSupport(
            IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache,
            IgniteCache<SparkShufflePartition, Long> metadataCache,
            ExecutorService blockFetcherExecutor,
            String appId,
            int numBlocksPerBatch,
            int maxBatchesInFlight) {
        this.dataCache = dataCache;
        this.metadataCache = metadataCache;
        this.blockFetcherExecutor = MoreExecutors.listeningDecorator(blockFetcherExecutor);
        this.appId = appId;
        this.numBlocksPerBatch = numBlocksPerBatch;
        this.maxBatchesInFlight = maxBatchesInFlight;
    }

    @Override
    public Iterable<InputStream> getPartitionReaders(Iterable<ShuffleBlockInfo> blockMetadata) {
        return () -> {
            List<SparkShufflePartition> partitionKeys = StreamSupport.stream(
                    blockMetadata.spliterator(), false)
                    .map(blockMeta ->
                        SparkShufflePartition.builder()
                                .shuffleId(blockMeta.getShuffleId())
                                .mapId(blockMeta.getMapId())
                                .partitionId(blockMeta.getReduceId())
                                .appId(appId)
                                .build())
                    .collect(Collectors.toList());
            Map<SparkShufflePartition, Long> numBlocksPerPartition =
                    metadataCache.getAll(ImmutableSet.copyOf(partitionKeys));
            return new IgniteStreamIterator(partitionKeys, numBlocksPerPartition);
        };
    }

    private final class IgniteStreamIterator implements Iterator<InputStream> {
        private final Deque<SparkShufflePartition> partitionsToReturn;
        private final Deque<SparkShufflePartition> partitionsToFetch;
        private final Map<SparkShufflePartition, Long> numBlocksPerPartition;
        private final BlockingDeque<IgniteFuture<Map<SparkShufflePartitionBlock, byte[]>>> outstandingBlockRequests;
        private final AtomicBoolean isStopped = new AtomicBoolean(false);
        private final Map<SparkShufflePartitionBlock, byte[]> currentBufferedBlocks;
        private ListenableFuture<?> blockFetchTask;

        private IgniteStreamIterator(
                Iterable<SparkShufflePartition> partitionsToFetch,
                Map<SparkShufflePartition, Long> numBlocksPerPartition) {
            this.partitionsToFetch = Lists.newLinkedList(partitionsToFetch);
            this.partitionsToReturn = Lists.newLinkedList(partitionsToFetch);
            this.numBlocksPerPartition = numBlocksPerPartition;
            this.currentBufferedBlocks = Maps.newHashMapWithExpectedSize(numBlocksPerBatch);
            this.outstandingBlockRequests = Queues.newLinkedBlockingDeque(maxBatchesInFlight);
        }

        private void stop() {
            isStopped.set(true);
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = !partitionsToReturn.isEmpty();
            if (!hasNext) {
                stop();
            }
            return hasNext && !isStopped.get();
        }

        @Override
        public InputStream next() {
            if (blockFetchTask == null) {
                blockFetchTask = blockFetcherExecutor.submit(this::blockFetchLoop);
            }

            SparkShufflePartition partToReturn = partitionsToReturn.pop();
            if (!numBlocksPerPartition.containsKey(partToReturn)) {
                return new ByteArrayInputStream(new byte[]{});
            }
            Iterator<InputStream> partitionBlocksInputStream = new Iterator<InputStream>() {
                private int curBlockIndex = 0;
                @Override
                public boolean hasNext() {
                    return curBlockIndex < numBlocksPerPartition.get(partToReturn) && !isStopped.get();
                }

                @Override
                public InputStream next() {
                    try {
                        SparkShufflePartitionBlock blockKey = SparkShufflePartitionBlock.of(
                                partToReturn, curBlockIndex);
                        curBlockIndex++;
                        while (!currentBufferedBlocks.containsKey(blockKey) && !isStopped.get()) {
                            try {
                                currentBufferedBlocks.putAll(outstandingBlockRequests.take().get());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        byte[] blockBytes = !isStopped.get() ? currentBufferedBlocks.remove(blockKey) : new byte[] {};
                        return new ByteArrayInputStream(blockBytes);
                    } catch (Exception e) {
                        try {
                            stop();
                            blockFetchTask.cancel(true);
                        } catch (Exception e2) {
                            LOG.warn("Failed to cancel block fetch task upon failing to retrieve block.", e2);
                        }
                        throw e;
                    }
                }
            };
            return new SequenceInputStream(new IteratorEnumeration<>(partitionBlocksInputStream));
        }

        private void blockFetchLoop() {
            Set<SparkShufflePartitionBlock> currentBlocksToFetch =
                    Sets.newHashSetWithExpectedSize(numBlocksPerBatch);

            while (!isStopped.get() && !partitionsToFetch.isEmpty()) {
                SparkShufflePartition currentPartition = partitionsToFetch.pop();
                long numBlocksInPartition = numBlocksPerPartition.get(currentPartition);
                for (long i = 0; i < numBlocksInPartition && !isStopped.get(); i++) {
                    currentBlocksToFetch.add(SparkShufflePartitionBlock.of(currentPartition, i));
                    if (currentBlocksToFetch.size() == numBlocksPerBatch && !isStopped.get()) {
                        pushBlocks(currentBlocksToFetch);
                        currentBlocksToFetch.clear();
                    }
                }
            }

            if (!currentBlocksToFetch.isEmpty() && !isStopped.get()) {
                pushBlocks(currentBlocksToFetch);
            }
        }

        private void pushBlocks(Set<SparkShufflePartitionBlock> currentBlocksToFetch) {
            try {
                outstandingBlockRequests.put(dataCache.getAllAsync(ImmutableSet.copyOf(currentBlocksToFetch)));
            } catch (InterruptedException e) {
                if (!isStopped.get()) {
                    throw new RuntimeException(e);
                } else {
                    LOG.warn(
                            "Fetch interrupted but the fetch process should have already been stopped.", e);
                }
            }
        }
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
