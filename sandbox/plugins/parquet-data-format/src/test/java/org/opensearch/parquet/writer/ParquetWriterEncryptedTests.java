/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.writer;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.parquet.encryption.PmeDataKey;
import org.opensearch.parquet.encryption.PmeFileEncryptionInputs;
import org.opensearch.parquet.encryption.PmeFileKeyMetadata;
import org.opensearch.parquet.encryption.PmeKeyDerivation;
import org.opensearch.parquet.engine.ParquetDataFormat;
import org.opensearch.parquet.fields.ArrowFieldRegistry;
import org.opensearch.parquet.fields.ParquetField;
import org.opensearch.parquet.memory.ArrowBufferPool;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Encrypted variants of {@link ParquetWriterTests}.
 *
 * <p>Each test writes a Parquet file with PME encryption enabled and then reads it back
 * using the derived footer key, verifying that:
 * <ul>
 *   <li>The file is unreadable without the correct key ({@code getFileMetadata} throws).</li>
 *   <li>The file is readable with the correct key and row counts match.</li>
 *   <li>The key metadata JSON ({@code version}, {@code data_key_id}, {@code message_id}) is
 *       persisted in the Parquet footer and can be round-tripped through
 *       {@link PmeFileKeyMetadata#parse}.</li>
 * </ul>
 */
public class ParquetWriterEncryptedTests extends OpenSearchTestCase {

    private ArrowBufferPool bufferPool;
    private MappedFieldType idField;
    private MappedFieldType nameField;
    private MappedFieldType scoreField;
    private Schema schema;
    private ThreadPool threadPool;

    /** A fixed data key reused across tests in this class (32 random bytes, set in setUp). */
    private byte[] dataKey;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        RustBridge.initLogger();
        bufferPool = new ArrowBufferPool(Settings.EMPTY);
        idField = new NumberFieldMapper.NumberFieldType("id", NumberFieldMapper.NumberType.INTEGER);
        nameField = new KeywordFieldMapper.KeywordFieldType("name");
        scoreField = new NumberFieldMapper.NumberFieldType("score", NumberFieldMapper.NumberType.LONG);
        schema = buildSchema(List.of(idField, nameField, scoreField));
        Settings settings = Settings.builder().put("node.name", "parquetwriter-encrypted-test").build();
        threadPool = new ThreadPool(
            settings,
            new FixedExecutorBuilder(
                settings,
                ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME,
                1,
                -1,
                "thread_pool." + ParquetDataFormatPlugin.PARQUET_THREAD_POOL_NAME
            )
        );
        dataKey = randomByteArrayOfLength(PmeKeyDerivation.DATA_KEY_BYTES);
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        bufferPool.close();
        super.tearDown();
    }

    // ---- helpers ----

    /**
     * Creates a fresh {@link PmeFileEncryptionInputs} from the test data key and a new random
     * message_id, matching what {@link org.opensearch.parquet.encryption.PmeContext} does on the
     * real write path.
     */
    private PmeFileEncryptionInputs encryptionInputs() {
        return PmeFileEncryptionInputs.create(new PmeDataKey(dataKey));
    }

    /**
     * Reconstructs the decryption inputs from the key metadata JSON stored in the Parquet footer.
     * This is the read path: parse the footer JSON → derive footer key + AAD prefix from data key
     * and message_id.
     */
    private PmeFileEncryptionInputs decryptionInputs(String filePath) throws Exception {
        byte[] keyMetadataBytes = RustBridge.readKeyMetadata(filePath);
        assertNotNull("Encrypted file must contain key_metadata", keyMetadataBytes);
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(keyMetadataBytes);
        assertEquals(PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, meta.dataKeyId());
        byte[] footerKey = PmeKeyDerivation.deriveFooterKey(dataKey, meta.messageId());
        byte[] aadPrefix = PmeKeyDerivation.buildAadPrefix(meta.messageId());
        return PmeFileEncryptionInputs.forDecryption(footerKey, aadPrefix);
    }

    private Schema buildSchema(List<MappedFieldType> fieldTypes) {
        List<Field> fields = new ArrayList<>();
        for (MappedFieldType ft : fieldTypes) {
            ParquetField pf = ArrowFieldRegistry.getParquetField(ft.typeName());
            assertNotNull("No ParquetField registered for type: " + ft.typeName(), pf);
            fields.add(new Field(ft.name(), pf.getFieldType(), null));
        }
        return new Schema(fields);
    }

    // ---- tests ----

    /**
     * Mirror of {@link ParquetWriterTests#testSingleDocumentFlush} with PME enabled.
     * Verifies the encrypted file is readable with the correct key and unreadable without it.
     */
    public void testEncryptedSingleDocumentFlush() throws Exception {
        String filePath = createTempDir().resolve("encrypted-single.parquet").toString();
        PmeFileEncryptionInputs enc = encryptionInputs();

        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            new ParquetDataFormat(),
            schema,
            bufferPool,
            Settings.EMPTY,
            threadPool,
            null,
            enc
        );

        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 42);
        doc.addField(nameField, "alice");
        doc.addField(scoreField, 500L);
        writer.addDoc(doc);
        doc.close();
        writer.flush();

        assertTrue("Encrypted parquet file must exist", Files.exists(Path.of(filePath)));

        // Plain read must fail — file is encrypted.
        expectThrows(Exception.class, () -> RustBridge.getFileMetadata(filePath));

        // Read with correct key reconstructed from footer metadata.
        PmeFileEncryptionInputs dec = decryptionInputs(filePath);
        assertEquals(1, RustBridge.getFileMetadata(filePath, dec).numRows());
    }

    /**
     * Mirror of {@link ParquetWriterTests#testMultipleDocumentsFlush} with PME enabled.
     */
    public void testEncryptedMultipleDocumentsFlush() throws Exception {
        String filePath = createTempDir().resolve("encrypted-multi.parquet").toString();
        PmeFileEncryptionInputs enc = encryptionInputs();

        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            new ParquetDataFormat(),
            schema,
            bufferPool,
            Settings.EMPTY,
            threadPool,
            null,
            enc
        );

        int count = randomIntBetween(2, 20);
        for (int i = 0; i < count; i++) {
            ParquetDocumentInput doc = new ParquetDocumentInput();
            doc.addField(idField, i);
            doc.addField(nameField, "user_" + i);
            doc.addField(scoreField, (long) (i * 100));
            writer.addDoc(doc);
            doc.close();
        }

        FileInfos fileInfos = writer.flush();
        assertNotNull(fileInfos);
        assertTrue(Files.exists(Path.of(filePath)));

        expectThrows(Exception.class, () -> RustBridge.getFileMetadata(filePath));

        PmeFileEncryptionInputs dec = decryptionInputs(filePath);
        assertEquals(count, RustBridge.getFileMetadata(filePath, dec).numRows());
    }

    /**
     * Verifies that a wrong data key (different {@code dataKey} but same file) fails to decrypt.
     */
    public void testEncryptedReadWithWrongKeyFails() throws Exception {
        String filePath = createTempDir().resolve("encrypted-wrong-key.parquet").toString();
        PmeFileEncryptionInputs enc = encryptionInputs();

        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            new ParquetDataFormat(),
            schema,
            bufferPool,
            Settings.EMPTY,
            threadPool,
            null,
            enc
        );
        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 1);
        doc.addField(nameField, "bob");
        doc.addField(scoreField, 99L);
        writer.addDoc(doc);
        doc.close();
        writer.flush();

        // Reconstruct inputs using a different (wrong) data key.
        byte[] keyMetadataBytes = RustBridge.readKeyMetadata(filePath);
        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(keyMetadataBytes);
        byte[] wrongDataKey = randomByteArrayOfLength(PmeKeyDerivation.DATA_KEY_BYTES);
        byte[] wrongFooterKey = PmeKeyDerivation.deriveFooterKey(wrongDataKey, meta.messageId());
        byte[] aadPrefix = PmeKeyDerivation.buildAadPrefix(meta.messageId());
        PmeFileEncryptionInputs wrongDec = PmeFileEncryptionInputs.forDecryption(wrongFooterKey, aadPrefix);

        expectThrows(Exception.class, () -> RustBridge.getDecryptedNumRows(filePath, wrongDec));
    }

    /**
     * Verifies that the key metadata JSON stored in the Parquet footer round-trips correctly
     * through {@link PmeFileKeyMetadata#parse}: version=1, data_key_id="default",
     * message_id decodes to 16 bytes.
     */
    public void testEncryptedFooterKeyMetadataRoundTrip() throws Exception {
        String filePath = createTempDir().resolve("encrypted-meta.parquet").toString();
        PmeFileEncryptionInputs enc = encryptionInputs();

        ParquetWriter writer = new ParquetWriter(
            filePath,
            1L,
            new ParquetDataFormat(),
            schema,
            bufferPool,
            Settings.EMPTY,
            threadPool,
            null,
            enc
        );
        ParquetDocumentInput doc = new ParquetDocumentInput();
        doc.addField(idField, 7);
        doc.addField(nameField, "meta-test");
        doc.addField(scoreField, 777L);
        writer.addDoc(doc);
        doc.close();
        writer.flush();

        byte[] keyMetadataBytes = RustBridge.readKeyMetadata(filePath);
        assertNotNull(keyMetadataBytes);

        PmeFileKeyMetadata meta = PmeFileKeyMetadata.parse(keyMetadataBytes);
        assertEquals(1, meta.version());
        assertEquals(PmeFileKeyMetadata.DEFAULT_DATA_KEY_ID, meta.dataKeyId());
        assertEquals(16, meta.messageId().length);
    }
}

