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
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;

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
    private static final MethodHandle READ_KEY_METADATA;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        // parquet_create_writer(file, file_len, schema_address,
        //   footer_key, footer_key_len, key_metadata_json, key_metadata_json_len,
        //   aad_prefix, aad_prefix_len) -> i64
        CREATE_WRITER = linker.downcallHandle(
            lib.find("parquet_create_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // file
                ValueLayout.JAVA_LONG,                        // schema_address
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // footer_key
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // key_metadata_json
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG   // aad_prefix
            )
        );
        WRITE = linker.downcallHandle(
            lib.find("parquet_write").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG
            )
        );
        FINALIZE_WRITER = linker.downcallHandle(
            lib.find("parquet_finalize_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS
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
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        // parquet_get_file_metadata_decrypted(file, footer_key, footer_key_len,
        //   aad_prefix, aad_prefix_len, version_out, num_rows_out, created_by_buf,
        //   created_by_buf_len, created_by_len_out) -> i64
        GET_FILE_METADATA_DECRYPTED = linker.downcallHandle(
            lib.find("parquet_get_file_metadata_decrypted").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // file
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // footer_key
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // aad_prefix
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,     // version_out, num_rows_out
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // created_by_buf, len
                ValueLayout.ADDRESS                           // created_by_len_out
            )
        );
        // parquet_get_decrypted_num_rows(file, footer_key, footer_key_len,
        //   aad_prefix, aad_prefix_len, num_rows_out) -> i64
        GET_DECRYPTED_NUM_ROWS = linker.downcallHandle(
            lib.find("parquet_get_decrypted_num_rows").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // file
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // footer_key
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // aad_prefix
                ValueLayout.ADDRESS                           // num_rows_out
            )
        );
        GET_FILTERED_BYTES = linker.downcallHandle(
            lib.find("parquet_get_filtered_native_bytes_used").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        // parquet_read_key_metadata(file, file_len, out_buf, out_buf_len, out_len_out) -> i64
        // Returns 0 if key_metadata present, 1 if file not encrypted / no key_metadata.
        READ_KEY_METADATA = linker.downcallHandle(
            lib.find("parquet_read_key_metadata").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // file
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // out_buf
                ValueLayout.ADDRESS                           // out_len_out
            )
        );
    }

    public static void initLogger() {}

    /**
     * Creates a native Parquet writer for the given file.
     *
     * @param file             path to the Parquet file to create
     * @param schemaAddress    native memory address of the Arrow schema
     * @param encryptionInputs per-file PME inputs (footer key, key metadata JSON, AAD prefix),
     *                         or {@code null} for an unencrypted file
     * @throws IOException if the native writer creation fails
     */
    static void createWriter(String file, long schemaAddress, PmeFileEncryptionInputs encryptionInputs) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            if (encryptionInputs == null) {
                call.invokeIO(
                    CREATE_WRITER,
                    f.segment(), f.len(),
                    schemaAddress,
                    MemorySegment.NULL, 0L,
                    MemorySegment.NULL, 0L,
                    MemorySegment.NULL, 0L
                );
                return;
            }
            byte[] footerKeyBytes = encryptionInputs.footerKey();
            byte[] keyMetaBytes = encryptionInputs.keyMetadataJson();
            byte[] aadBytes = encryptionInputs.aadPrefix();
            var footerKey = call.bytes(footerKeyBytes);
            var keyMeta = call.bytes(keyMetaBytes);
            var aadPrefix = call.bytes(aadBytes);
            call.invokeIO(
                CREATE_WRITER,
                f.segment(), f.len(),
                schemaAddress,
                footerKey, (long) footerKeyBytes.length,
                keyMeta, (long) keyMetaBytes.length,
                aadPrefix, (long) aadBytes.length
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
                f.segment(), f.len(),
                versionOut, numRowsOut,
                out.data(), (long) out.capacity(),
                out.lenOut(), crc32Out
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

    /**
     * Reads the encrypted Parquet file metadata using the provided per-file encryption inputs.
     *
     * @param file             path to the encrypted Parquet file
     * @param encryptionInputs per-file PME inputs containing the derived footer key and AAD prefix
     * @return file metadata
     * @throws IOException if reading or decryption fails
     */
    public static ParquetFileMetadata getFileMetadata(String file, PmeFileEncryptionInputs encryptionInputs) throws IOException {
        if (encryptionInputs == null) {
            return getFileMetadata(file);
        }
        try (var call = new NativeCall()) {
            var f = call.str(file);
            byte[] footerKeyBytes = encryptionInputs.footerKey();
            byte[] aadBytes = encryptionInputs.aadPrefix();
            var footerKey = call.bytes(footerKeyBytes);
            var aadPrefix = call.bytes(aadBytes);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var out = call.outBuffer(1024);
            call.invokeIO(
                GET_FILE_METADATA_DECRYPTED,
                f.segment(), f.len(),
                footerKey, (long) footerKeyBytes.length,
                aadPrefix, (long) aadBytes.length,
                versionOut, numRowsOut,
                out.data(), (long) out.capacity(),
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

    /**
     * Returns the decrypted row count for an encrypted Parquet file.
     *
     * @param file             path to the encrypted Parquet file
     * @param encryptionInputs per-file PME inputs containing the derived footer key and AAD prefix
     * @return total row count
     * @throws IOException if reading or decryption fails
     */
    public static long getDecryptedNumRows(String file, PmeFileEncryptionInputs encryptionInputs) throws IOException {
        if (encryptionInputs == null) {
            return getFileMetadata(file).numRows();
        }
        try (var call = new NativeCall()) {
            var f = call.str(file);
            byte[] footerKeyBytes = encryptionInputs.footerKey();
            byte[] aadBytes = encryptionInputs.aadPrefix();
            var footerKey = call.bytes(footerKeyBytes);
            var aadPrefix = call.bytes(aadBytes);
            var numRowsOut = call.longOut();
            call.invokeIO(
                GET_DECRYPTED_NUM_ROWS,
                f.segment(), f.len(),
                footerKey, (long) footerKeyBytes.length,
                aadPrefix, (long) aadBytes.length,
                numRowsOut
            );
            return numRowsOut.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    /**
     * Reads the plaintext {@code key_metadata} bytes from an encrypted Parquet file's
     * {@code FileCryptoMetaData} without decrypting the footer.
     *
     * <p>Returns {@code null} if the file is not encrypted or has no {@code key_metadata}.
     *
     * @param file path to the Parquet file
     * @return UTF-8 JSON key metadata bytes, or {@code null}
     * @throws IOException if the file cannot be read or is malformed
     */
    public static byte[] readKeyMetadata(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var out = call.outBuffer(256);
            long rc = call.invokeIO(
                READ_KEY_METADATA,
                f.segment(), f.len(),
                out.data(), (long) out.capacity(),
                out.lenOut()
            );
            if (rc == 1) {
                // Not encrypted or no key_metadata.
                return null;
            }
            int len = out.actualLength();
            if (len < 0) {
                return null;
            }
            return out.data().asSlice(0, len).toArray(ValueLayout.JAVA_BYTE);
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
