/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

public class RustBridge {

    private static final MethodHandle CREATE_WRITER;
    private static final MethodHandle WRITE;
    private static final MethodHandle FINALIZE_WRITER;
    private static final MethodHandle SYNC_TO_DISK;
    private static final MethodHandle GET_FILE_METADATA;
    private static final MethodHandle GET_FILE_METADATA_DECRYPTED;
    private static final MethodHandle GET_DECRYPTED_NUM_ROWS;
    private static final MethodHandle GET_FILTERED_BYTES;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        CREATE_WRITER = linker.downcallHandle(
            lib.find("parquet_create_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );
        WRITE = linker.downcallHandle(
            lib.find("parquet_write").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );
        FINALIZE_WRITER = linker.downcallHandle(
            lib.find("parquet_finalize_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS
            )
        );
        SYNC_TO_DISK = linker.downcallHandle(
            lib.find("parquet_sync_to_disk").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        GET_FILE_METADATA = linker.downcallHandle(
            lib.find("parquet_get_file_metadata").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        GET_FILE_METADATA_DECRYPTED = linker.downcallHandle(
            lib.find("parquet_get_file_metadata_decrypted").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        GET_DECRYPTED_NUM_ROWS = linker.downcallHandle(
            lib.find("parquet_get_decrypted_num_rows").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        GET_FILTERED_BYTES = linker.downcallHandle(
            lib.find("parquet_get_filtered_native_bytes_used").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
    }

    public static void initLogger() {}

    static void createWriter(String file, long schemaAddress, ParquetModularEncryptionConfig encryptionConfig) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            if (encryptionConfig == null) {
                call.invokeIO(
                    CREATE_WRITER,
                    f.segment(),
                    f.len(),
                    schemaAddress,
                    MemorySegment.NULL,
                    0L,
                    MemorySegment.NULL,
                    0L,
                    MemorySegment.NULL,
                    0L,
                    MemorySegment.NULL,
                    0L,
                    MemorySegment.NULL,
                    0L,
                    MemorySegment.NULL,
                    0L
                );
                return;
            }

            // Prototyp-Entscheidung: Wir uebergeben rohes Footer-Key-Material plus den gewrappten
            // Footer-Key aus dem KMS. Damit kann Rust echte PME aktivieren und zugleich die KMS-Info
            // fuer spaetere Reader-Integration im Footer-Metadatum verankern.
            var kmsInstanceId = call.str(encryptionConfig.kmsInstanceId());
            var kmsInstanceType = call.str(encryptionConfig.kmsInstanceType());
            var kmsKeyArn = call.str(encryptionConfig.kmsKeyArn());
            var kmsEncryptionContext = call.str(encryptionConfig.kmsEncryptionContext());
            byte[] footerKeyBytes = encryptionConfig.footerKey();
            var footerKey = call.bytes(footerKeyBytes);
            byte[] wrappedFooterKeyBytes = encryptionConfig.wrappedFooterKey();
            var wrappedFooterKey = call.bytes(wrappedFooterKeyBytes);
            call.invokeIO(
                CREATE_WRITER,
                f.segment(),
                f.len(),
                schemaAddress,
                kmsInstanceId.segment(),
                kmsInstanceId.len(),
                kmsInstanceType.segment(),
                kmsInstanceType.len(),
                kmsKeyArn.segment(),
                kmsKeyArn.len(),
                kmsEncryptionContext.segment(),
                kmsEncryptionContext.len(),
                footerKey,
                (long) footerKeyBytes.length,
                wrappedFooterKey,
                (long) wrappedFooterKeyBytes.length
            );
        }
    }

    static void write(String file, long arrayAddress, long schemaAddress) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(WRITE, f.segment(), f.len(), arrayAddress, schemaAddress);
        }
    }

    static ParquetFileMetadata finalizeWriter(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var crc32Out = call.longOut();
            var out = call.outBuffer(1024);
            long rc = call.invokeIO(
                FINALIZE_WRITER,
                f.segment(),
                f.len(),
                versionOut,
                numRowsOut,
                out.data(),
                (long) out.capacity(),
                out.lenOut(),
                crc32Out
            );
            if (rc == 1) return null;
            int createdByLen = out.actualLength();
            return new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                crc32Out.get(ValueLayout.JAVA_LONG, 0)
            );
        }
    }

    static void syncToDisk(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(SYNC_TO_DISK, f.segment(), f.len());
        }
    }

    public static ParquetFileMetadata getFileMetadata(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var out = call.outBuffer(1024);
            call.invokeIO(GET_FILE_METADATA, f.segment(), f.len(), versionOut, numRowsOut, out.data(), (long) out.capacity(), out.lenOut());
            int createdByLen = out.actualLength();
            return new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                0L
            );
        }
    }

    public static ParquetFileMetadata getFileMetadata(String file, ParquetModularEncryptionConfig encryptionConfig) throws IOException {
        if (encryptionConfig == null) {
            return getFileMetadata(file);
        }
        try (var call = new NativeCall()) {
            var f = call.str(file);
            byte[] footerKeyBytes = encryptionConfig.footerKey();
            var footerKey = call.bytes(footerKeyBytes);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var out = call.outBuffer(1024);
            call.invokeIO(
                GET_FILE_METADATA_DECRYPTED,
                f.segment(),
                f.len(),
                footerKey,
                (long) footerKeyBytes.length,
                versionOut,
                numRowsOut,
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            int createdByLen = out.actualLength();
            return new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                0L
            );
        }
    }

    public static long getDecryptedNumRows(String file, ParquetModularEncryptionConfig encryptionConfig) throws IOException {
        if (encryptionConfig == null) {
            return getFileMetadata(file).numRows();
        }
        try (var call = new NativeCall()) {
            var f = call.str(file);
            byte[] footerKeyBytes = encryptionConfig.footerKey();
            var footerKey = call.bytes(footerKeyBytes);
            var numRowsOut = call.longOut();
            call.invokeIO(
                GET_DECRYPTED_NUM_ROWS,
                f.segment(),
                f.len(),
                footerKey,
                (long) footerKeyBytes.length,
                numRowsOut
            );
            return numRowsOut.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    public static long getFilteredNativeBytesUsed(String pathPrefix) {
        try (var call = new NativeCall()) {
            var p = call.str(pathPrefix);
            return call.invoke(GET_FILTERED_BYTES, p.segment(), p.len());
        }
    }

    private RustBridge() {}
}
