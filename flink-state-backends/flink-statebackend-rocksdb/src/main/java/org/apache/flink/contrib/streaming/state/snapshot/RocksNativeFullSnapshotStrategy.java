/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateUploader;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.getUploadedStateSize;

/**
 * Snapshot strategy for {@link RocksDBKeyedStateBackend} based on RocksDB's native checkpoints and
 * creates full snapshots. the difference between savepoint is that sst files will be uploaded
 * rather than states.
 *
 * @param <K> type of the backend keys.
 */
public class RocksNativeFullSnapshotStrategy<K>
        extends RocksDBSnapshotStrategyBase<
                K, RocksNativeFullSnapshotStrategy.NativeFullRocksDBSnapshotResources> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocksNativeFullSnapshotStrategy.class);

    private static final String DESCRIPTION = "Asynchronous full RocksDB snapshot";

    /** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
    @Nonnull private final UUID backendUID;

    /** The help class used to upload state files. */
    private final RocksDBStateUploader stateUploader;

    public RocksNativeFullSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull File instanceBasePath,
            @Nonnull UUID backendUID,
            @Nonnull RocksDBStateUploader rocksDBStateUploader) {
        super(
                DESCRIPTION,
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                localRecoveryConfig,
                instanceBasePath,
                backendUID.toString().replaceAll("[\\-]", ""));
        this.backendUID = backendUID;
        this.stateUploader = rocksDBStateUploader;
    }

    @Override
    public NativeFullRocksDBSnapshotResources syncPrepareResources(long checkpointId)
            throws Exception {

        final SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
        LOG.trace("Local RocksDB checkpoint goes to backup path {}.", snapshotDirectory);

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());

        for (Map.Entry<String, RocksDbKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }

        takeDBNativeCheckpoint(snapshotDirectory);

        return new NativeFullRocksDBSnapshotResources(snapshotDirectory, stateMetaInfoSnapshots);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            NativeFullRocksDBSnapshotResources snapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (snapshotResources.stateMetaInfoSnapshots.isEmpty()) {
            return registry -> SnapshotResult.empty();
        }

        return new RocksDBNativeFullSnapshotOperation(
                checkpointId,
                checkpointStreamFactory,
                snapshotResources.snapshotDirectory,
                snapshotResources.stateMetaInfoSnapshots);
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) {
        // nothing to do
    }

    @Override
    public void notifyCheckpointAborted(long abortedCheckpointId) {
        // nothing to do
    }

    private void takeDBNativeCheckpoint(@Nonnull SnapshotDirectory outputDirectory)
            throws Exception {
        // create hard links of living files in the output path
        try (ResourceGuard.Lease ignored = rocksDBResourceGuard.acquireResource();
                Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(outputDirectory.getDirectory().toString());
        } catch (Exception ex) {
            try {
                outputDirectory.cleanup();
            } catch (IOException cleanupEx) {
                ex = ExceptionUtils.firstOrSuppressed(cleanupEx, ex);
            }
            throw ex;
        }
    }

    @Override
    public void close() {
        stateUploader.close();
    }

    /** Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend. */
    private final class RocksDBNativeFullSnapshotOperation
            implements SnapshotResultSupplier<KeyedStateHandle> {

        /** Id for the current checkpoint. */
        private final long checkpointId;

        /** Stream factory that creates the output streams to DFS. */
        @Nonnull private final CheckpointStreamFactory checkpointStreamFactory;

        /** The state meta data. */
        @Nonnull private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        /** Local directory for the RocksDB native backup. */
        @Nonnull private final SnapshotDirectory localBackupDirectory;

        private RocksDBNativeFullSnapshotOperation(
                long checkpointId,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull SnapshotDirectory localBackupDirectory,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
            this.checkpointStreamFactory = checkpointStreamFactory;
            this.checkpointId = checkpointId;
            this.localBackupDirectory = localBackupDirectory;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {

            boolean completed = false;

            // Handle to the meta data file
            SnapshotResult<StreamStateHandle> metaStateHandle = null;
            // Handles to the misc files in the current snapshot will go here
            final Map<StateHandleID, StreamStateHandle> privateFiles = new HashMap<>();

            try {

                metaStateHandle = materializeMetaData(snapshotCloseableRegistry);

                // Sanity checks - they should never fail
                Preconditions.checkNotNull(metaStateHandle, "Metadata was not properly created.");
                Preconditions.checkNotNull(
                        metaStateHandle.getJobManagerOwnedSnapshot(),
                        "Metadata for job manager was not properly created.");

                uploadSstFiles(privateFiles, snapshotCloseableRegistry);
                long checkpointedSize = metaStateHandle.getStateSize();
                checkpointedSize += getUploadedStateSize(privateFiles.values());

                final IncrementalRemoteKeyedStateHandle jmIncrementalKeyedStateHandle =
                        new IncrementalRemoteKeyedStateHandle(
                                backendUID,
                                keyGroupRange,
                                checkpointId,
                                Collections.emptyMap(),
                                privateFiles,
                                metaStateHandle.getJobManagerOwnedSnapshot(),
                                checkpointedSize);

                final DirectoryStateHandle directoryStateHandle =
                        localBackupDirectory.completeSnapshotAndGetHandle();
                final SnapshotResult<KeyedStateHandle> snapshotResult;
                if (directoryStateHandle != null
                        && metaStateHandle.getTaskLocalSnapshot() != null) {

                    IncrementalLocalKeyedStateHandle localDirKeyedStateHandle =
                            new IncrementalLocalKeyedStateHandle(
                                    backendUID,
                                    checkpointId,
                                    directoryStateHandle,
                                    keyGroupRange,
                                    metaStateHandle.getTaskLocalSnapshot(),
                                    Collections.emptyMap());
                    snapshotResult =
                            SnapshotResult.withLocalState(
                                    jmIncrementalKeyedStateHandle, localDirKeyedStateHandle);
                } else {
                    snapshotResult = SnapshotResult.of(jmIncrementalKeyedStateHandle);
                }

                completed = true;

                return snapshotResult;
            } finally {
                if (!completed) {
                    final List<StateObject> statesToDiscard =
                            new ArrayList<>(1 + privateFiles.size());
                    statesToDiscard.add(metaStateHandle);
                    statesToDiscard.addAll(privateFiles.values());
                    cleanupIncompleteSnapshot(statesToDiscard);
                }
            }
        }

        private void cleanupIncompleteSnapshot(@Nonnull List<StateObject> statesToDiscard) {

            try {
                StateUtil.bestEffortDiscardAllStateObjects(statesToDiscard);
            } catch (Exception e) {
                LOG.warn("Could not properly discard states.", e);
            }

            if (localBackupDirectory.isSnapshotCompleted()) {
                try {
                    DirectoryStateHandle directoryStateHandle =
                            localBackupDirectory.completeSnapshotAndGetHandle();
                    if (directoryStateHandle != null) {
                        directoryStateHandle.discardState();
                    }
                } catch (Exception e) {
                    LOG.warn("Could not properly discard local state.", e);
                }
            }
        }

        @Nonnull
        private SnapshotResult<StreamStateHandle> materializeMetaData(
                @Nonnull CloseableRegistry snapshotCloseableRegistry) throws Exception {

            CheckpointStreamWithResultProvider streamWithResultProvider =
                    localRecoveryConfig.isLocalRecoveryEnabled()
                            ? CheckpointStreamWithResultProvider.createDuplicatingStream(
                                    checkpointId,
                                    CheckpointedStateScope.EXCLUSIVE,
                                    checkpointStreamFactory,
                                    localRecoveryConfig
                                            .getLocalStateDirectoryProvider()
                                            .orElseThrow(
                                                    LocalRecoveryConfig.localRecoveryNotEnabled()))
                            : CheckpointStreamWithResultProvider.createSimpleStream(
                                    CheckpointedStateScope.EXCLUSIVE, checkpointStreamFactory);

            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

            try {
                // no need for compression scheme support because sst-files are already compressed
                KeyedBackendSerializationProxy<K> serializationProxy =
                        new KeyedBackendSerializationProxy<>(
                                keySerializer, stateMetaInfoSnapshots, false);

                DataOutputView out =
                        new DataOutputViewStreamWrapper(
                                streamWithResultProvider.getCheckpointOutputStream());

                serializationProxy.write(out);

                if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                    SnapshotResult<StreamStateHandle> result =
                            streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                    streamWithResultProvider = null;
                    return result;
                } else {
                    throw new IOException("Stream already closed and cannot return a handle.");
                }
            } finally {
                if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                    IOUtils.closeQuietly(streamWithResultProvider);
                }
            }
        }

        protected void uploadSstFiles(
                @Nonnull Map<StateHandleID, StreamStateHandle> privateFiles,
                @Nonnull CloseableRegistry snapshotCloseableRegistry)
                throws Exception {

            // write state data
            Preconditions.checkState(localBackupDirectory.exists());

            Map<StateHandleID, Path> privateFilePaths = new HashMap<>();

            Path[] files = localBackupDirectory.listDirectory();
            if (files != null) {
                // all sst files are private in full snapshot
                for (Path filePath : files) {
                    final String fileName = filePath.getFileName().toString();
                    final StateHandleID stateHandleID = new StateHandleID(fileName);
                    privateFilePaths.put(stateHandleID, filePath);
                }

                privateFiles.putAll(
                        stateUploader.uploadFilesToCheckpointFs(
                                privateFilePaths,
                                checkpointStreamFactory,
                                CheckpointedStateScope.EXCLUSIVE,
                                snapshotCloseableRegistry));
            }
        }
    }

    static class NativeFullRocksDBSnapshotResources implements SnapshotResources {
        @Nonnull private final SnapshotDirectory snapshotDirectory;
        @Nonnull private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        public NativeFullRocksDBSnapshotResources(
                @Nonnull SnapshotDirectory snapshotDirectory,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
            this.snapshotDirectory = snapshotDirectory;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        }

        @Override
        public void release() {
            try {
                if (snapshotDirectory.exists()) {
                    boolean cleanupOk = snapshotDirectory.cleanup();

                    if (!cleanupOk) {
                        LOG.debug("Could not properly cleanup local RocksDB backup directory.");
                    }
                }
            } catch (IOException e) {
                LOG.warn("Could not properly cleanup local RocksDB backup directory.", e);
            }
        }
    }
}
