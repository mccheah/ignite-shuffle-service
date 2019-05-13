/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
 */

package com.palantir.ignite.spark.shuffle.io;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.ignite.SparkShufflePartition;
import com.palantir.ignite.SparkShufflePartitionBlock;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.shuffle.ShuffleBlockInfo;
import org.apache.spark.api.shuffle.ShuffleMapOutputWriter;
import org.apache.spark.api.shuffle.ShufflePartitionWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public final class IgniteTest {

    private static final String IGNITE_DATA_CACHE_NAME = "ignite-data";
    private static final String APP_ID = "ignite-test-12345";
    private static final int BLOCK_SIZE = 10;
    private static final int NUM_BLOCKS_PER_BATCH = 2;
    private static final int NUM_CONCURRENT_BATCHES = 2;
    private static final int SHUFFLE_ID = 0;
    private static final int MAP_ID = 1;
    private static final byte[] PART_0_CONTENTS = generateData(25);
    private static final byte[] PART_1_CONTENTS = generateData(17);
    private static final byte[] PART_3_CONTENTS = generateData(1);

    private Ignite ignite;
    private IgniteCache<SparkShufflePartitionBlock, byte[]> dataCache;
    private IgniteCache<SparkShufflePartition, Long> metadataCache;
    private IgniteWriteSupport writeSupport;
    private IgniteReadSupport readSupport;
    private ExecutorService blockFetchExecutor;

    @Before
    public void before() {
        ignite = Ignition.start();
        dataCache = ignite.getOrCreateCache(IGNITE_DATA_CACHE_NAME);
        metadataCache = ignite.getOrCreateCache("shuffle-metadata");
        blockFetchExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ignite-block-fetch").build());
        writeSupport = new IgniteWriteSupport(
                dataCache,
                metadataCache,
                () -> ignite.dataStreamer(IGNITE_DATA_CACHE_NAME),
                () -> ignite.dataStreamer(metadataCache.getName()),
                APP_ID);
        readSupport = new IgniteReadSupport(
                dataCache, metadataCache, blockFetchExecutor, APP_ID, NUM_BLOCKS_PER_BATCH, NUM_CONCURRENT_BATCHES);
    }

    @After
    public void after() {
        dataCache.destroy();
        metadataCache.destroy();
        ignite.close();
        blockFetchExecutor.shutdownNow();
    }

    @Test
    public void testWriteAndRead() throws IOException {
        ShuffleMapOutputWriter mapOutput = writeSupport.createMapOutputWriter(SHUFFLE_ID, MAP_ID, 4);
        try (ShufflePartitionWriter partWriter = mapOutput.getPartitionWriter(0)) {
            partWriter.toStream().write(PART_0_CONTENTS);
        }

        try (ShufflePartitionWriter partWriter = mapOutput.getPartitionWriter(1)) {
            partWriter.toStream().write(PART_1_CONTENTS);
        }

        try (ShufflePartitionWriter partWriter = mapOutput.getPartitionWriter(3)) {
            partWriter.toStream().write(PART_3_CONTENTS);
        }

        mapOutput.commitAllPartitions();

        Iterator<InputStream> partInputs = readSupport.getPartitionReaders(ImmutableList.of(
                new ShuffleBlockInfo(SHUFFLE_ID, MAP_ID, 0, 0L, Optional.empty()),
                new ShuffleBlockInfo(SHUFFLE_ID, MAP_ID, 1, 0L, Optional.empty())))
                .iterator();
        assertThat(partInputs).hasNext();
        InputStream firstPart = partInputs.next();
        byte[] firstPartBytes = ByteStreams.toByteArray(firstPart);
        assertThat(firstPartBytes).containsExactly(PART_0_CONTENTS);
        InputStream secondPart = partInputs.next();
        byte[] secondPartBytes = ByteStreams.toByteArray(secondPart);
        assertThat(secondPartBytes).containsExactly(PART_1_CONTENTS);
        assertThat(partInputs).isExhausted();

        Iterator<InputStream> withEmptyBlock = readSupport.getPartitionReaders(ImmutableList.of(
                new ShuffleBlockInfo(SHUFFLE_ID, MAP_ID, 2, 0L, Optional.empty()))).iterator();
        assertThat(withEmptyBlock).hasNext();
        assertThat(ByteStreams.toByteArray(withEmptyBlock.next())).hasSize(0);
    }

    private static byte[] generateData(int numInts) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objOut = new ObjectOutputStream(output)) {
            for (int i = 0; i < numInts; i++) {
                objOut.writeInt(i);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return output.toByteArray();
    }
}
