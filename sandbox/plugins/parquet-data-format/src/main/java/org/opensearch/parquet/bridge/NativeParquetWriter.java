/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.common.SetOnce;
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Type-safe handle for the native Rust Parquet writer with lifecycle management.
 *
 * <p>Wraps the stateless FFM methods in {@link RustBridge} with a file-scoped lifecycle:
 * <ol>
 *   <li>{@code new NativeParquetWriter(filePath, schemaAddress, encryptionInputs)} — creates
 *       the native writer; zeroes {@code encryptionInputs.footerKey} on return.</li>
 *   <li>{@link #write(long, long)} — sends one or more Arrow batches (repeatable)</li>
 *   <li>{@link #flush()} — finalizes the Parquet file and returns metadata</li>
 *   <li>{@link #sync()} — fsyncs the file to durable storage (calls flush if needed)</li>
 * </ol>
 *
 * <p>This class is not thread-safe. External synchronization is required if instances are
 * shared across threads.
 */
public class NativeParquetWriter {

    private final AtomicBoolean writerFlushed = new AtomicBoolean(false);
    private final String filePath;
    private final SetOnce<ParquetFileMetadata> metadata = new SetOnce<>();

    /**
     * Creates a new NativeParquetWriter for an unencrypted file.
     *
     * @param filePath      the path to the Parquet file to write
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the native writer creation fails
     */
    public NativeParquetWriter(String filePath, long schemaAddress) throws IOException {
        this(filePath, schemaAddress, null);
    }

    /**
     * Creates a new NativeParquetWriter with optional PME encryption.
     *
     * <p>If {@code encryptionInputs} is non-null, its footer key is zeroed immediately after
     * the native writer is created so derived key material does not remain on the heap.
     *
     * @param filePath         the path to the Parquet file to write
     * @param schemaAddress    the native memory address of the Arrow schema
     * @param encryptionInputs per-file PME inputs; {@code null} keeps the plaintext path
     * @throws IOException if the native writer creation fails
     */
    public NativeParquetWriter(String filePath, long schemaAddress, PmeFileEncryptionInputs encryptionInputs) throws IOException {
        this.filePath = filePath;
        try {
            RustBridge.createWriter(filePath, schemaAddress, encryptionInputs);
        } finally {
            // Best-effort zero of the derived footer key regardless of success/failure.
            if (encryptionInputs != null) {
                encryptionInputs.zero();
            }
        }
    }

    /**
     * Writes an Arrow batch to the Parquet file.
     *
     * @param arrayAddress  the native memory address of the Arrow array
     * @param schemaAddress the native memory address of the Arrow schema
     * @throws IOException if the write fails or the writer has already been flushed
     */
    public void write(long arrayAddress, long schemaAddress) throws IOException {
        if (writerFlushed.get()) {
            throw new IOException("Cannot write to flushed Parquet writer: " + filePath);
        }
        RustBridge.write(filePath, arrayAddress, schemaAddress);
    }

    /**
     * Finalizes the Parquet file and returns metadata.
     *
     * @return the file metadata
     * @throws IOException if the finalization fails
     */
    public ParquetFileMetadata flush() throws IOException {
        if (writerFlushed.compareAndSet(false, true)) {
            metadata.set(RustBridge.finalizeWriter(filePath));
        }
        return metadata.get();
    }

    /**
     * Syncs the Parquet file to disk. Calls {@link #flush()} first if not already flushed.
     *
     * @throws IOException if the sync fails
     */
    public void sync() throws IOException {
        if (writerFlushed.get() == false) {
            flush();
        }
        RustBridge.syncToDisk(filePath);
    }

    /**
     * Returns the Parquet file metadata captured after flushing the writer.
     *
     * @return the file metadata, or null if the writer has not been flushed
     */
    public ParquetFileMetadata getMetadata() {
        return metadata.get();
    }
}
