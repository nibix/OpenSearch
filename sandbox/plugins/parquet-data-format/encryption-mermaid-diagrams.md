# Encryption Control Flow and Component Diagrams

These diagrams summarize the current Parquet PME prototype and the Lucene storage encryption
implementation used for comparison.

## Parquet PME: Engine Bootstrap and Writer Creation

```mermaid
sequenceDiagram
    autonumber
    participant OS as OpenSearch data-format framework
    participant Engine as ParquetIndexingEngine
    participant Crypto as CryptoHandlerRegistry
    participant KP as CryptoKeyProviderPlugin
    participant MKP as MasterKeyProvider
    participant Writer as ParquetWriter
    participant VSR as VSRManager
    participant Native as NativeParquetWriter
    participant Bridge as RustBridge
    participant Rust as Rust parquet_create_writer
    participant PWriter as Rust ArrowWriter

    OS->>Engine: indexingEngine(config, checksumStrategy)
    Engine->>Engine: CryptoMetadata.fromIndexSettings(indexSettings)
    alt no crypto metadata
        Engine->>Engine: encryptionConfig = null
    else crypto metadata present
        Engine->>Crypto: getInstance()
        Engine->>Crypto: fetchCryptoHandler(cryptoMetadata)
        Engine->>Crypto: getCryptoKeyProviderPlugin(type)
        Crypto-->>Engine: key provider plugin
        Engine->>KP: createKeyProvider(cryptoMetadata)
        KP-->>Engine: MasterKeyProvider
        Engine->>MKP: generateDataPair()
        MKP-->>Engine: rawKey + encryptedKey
        Engine->>Engine: new ParquetModularEncryptionConfig(...)
        Engine->>MKP: close()
    end
    OS->>Engine: createWriter(writerGeneration)
    Engine->>Writer: new ParquetWriter(..., encryptionConfig)
    Writer->>VSR: new VSRManager(..., encryptionConfig)
    VSR->>Native: new NativeParquetWriter(file, schemaAddress, encryptionConfig)
    Native->>Bridge: createWriter(file, schemaAddress, encryptionConfig)
    Bridge->>Rust: parquet_create_writer(..., KMS fields, footerKey, wrappedFooterKey)
    Rust->>Rust: validate all encryption fields together
    Rust->>PWriter: FileEncryptionProperties.builder(footerKey)
    Rust->>PWriter: with_footer_key_metadata(wrappedFooterKey)
    Rust->>PWriter: ArrowWriter::try_new(...)
```

## Parquet PME: Write, Flush, Sync

```mermaid
sequenceDiagram
    autonumber
    participant Caller as Indexing caller
    participant Writer as ParquetWriter
    participant VSR as VSRManager
    participant Pool as VSRPool / ManagedVSR
    participant Native as NativeParquetWriter
    participant Bridge as RustBridge
    participant Rust as Rust writer.rs
    participant File as Encrypted Parquet file
    participant Commit as WriterFileSet metadata

    Caller->>Writer: addDoc(ParquetDocumentInput)
    Writer->>VSR: addDocument(doc)
    VSR->>Pool: write fields into active Arrow vectors
    VSR->>Pool: maybeRotateActiveVSR()
    alt VSR rotation
        VSR->>Pool: freeze active VSR
        VSR->>Native: write(arrayAddress, schemaAddress)
        Native->>Bridge: parquet_write(file, array, schema)
        Bridge->>Rust: parquet_write(...)
        Rust->>Rust: create RecordBatch
        Rust->>File: ArrowWriter.write(recordBatch)
    end
    Caller->>Writer: flush()
    Writer->>VSR: flush()
    VSR->>VSR: await pending native write
    VSR->>Pool: freeze current VSR
    VSR->>Native: write(arrayAddress, schemaAddress)
    Native->>Bridge: parquet_write(...)
    Bridge->>Rust: parquet_write(...)
    Rust->>File: encrypted Parquet row groups/pages
    VSR->>Native: flush()
    Native->>Bridge: finalizeWriter(file)
    Bridge->>Rust: parquet_finalize_writer(...)
    Rust->>File: ArrowWriter.finish(), encrypted footer
    Rust-->>Bridge: metadata + crc32
    Bridge-->>Native: ParquetFileMetadata
    Native-->>VSR: ParquetFileMetadata
    VSR-->>Writer: ParquetFileMetadata
    Writer->>Commit: add file, rows, crc32
    alt encryptionConfig present
        Writer->>Commit: add PME metadata and wrapped footer key
    end
    Caller->>Writer: sync()
    Writer->>VSR: sync()
    VSR->>Native: sync()
    Native->>Bridge: syncToDisk(file)
    Bridge->>Rust: parquet_sync_to_disk(file)
    Rust->>File: fsync
```

## Parquet PME: Current Prototype Decrypt Read

```mermaid
sequenceDiagram
    autonumber
    participant Test as Java test / bridge caller
    participant Bridge as RustBridge
    participant RustFFM as ffm.rs
    participant RustReader as writer.rs read helpers
    participant Parquet as Apache Parquet reader
    participant File as Encrypted Parquet file

    Test->>Bridge: getFileMetadata(file, encryptionConfig)
    Bridge->>Bridge: extract raw footerKey from config
    Bridge->>RustFFM: parquet_get_file_metadata_decrypted(file, footerKey)
    RustFFM->>RustFFM: require non-empty footerKey
    RustFFM->>RustReader: get_file_metadata_decrypted(file, footerKey)
    RustReader->>Parquet: FileDecryptionProperties.builder(footerKey)
    RustReader->>Parquet: ArrowReaderMetadata::load(file, options)
    Parquet->>File: decrypt footer metadata
    File-->>Parquet: metadata
    Parquet-->>RustReader: FileMetaData
    RustReader-->>Bridge: version, rows, created_by
    Bridge-->>Test: ParquetFileMetadata

    Test->>Bridge: getDecryptedNumRows(file, encryptionConfig)
    Bridge->>RustFFM: parquet_get_decrypted_num_rows(file, footerKey)
    RustFFM->>RustReader: get_decrypted_num_rows(file, footerKey)
    RustReader->>Parquet: ParquetRecordBatchReaderBuilder with decryption options
    loop each decrypted batch
        Parquet->>File: read and decrypt payload
        Parquet-->>RustReader: RecordBatch
        RustReader->>RustReader: add batch.num_rows()
    end
    RustReader-->>Bridge: row count
    Bridge-->>Test: row count
```

## Parquet PME: Missing Production Reader Rehydration

```mermaid
sequenceDiagram
    autonumber
    participant Query as Query/execution reader
    participant Meta as PME metadata parser
    participant Crypto as CryptoHandlerRegistry
    participant KP as CryptoKeyProviderPlugin
    participant MKP as MasterKeyProvider
    participant Bridge as RustBridge
    participant Rust as Rust decrypt reader
    participant File as Encrypted Parquet file

    Query->>Meta: load PME metadata for file
    Meta->>File: read wrapped footer key and provider metadata
    Meta->>Meta: validate version, provider, key id, context hash
    Meta->>Crypto: getCryptoKeyProviderPlugin(provider type)
    Crypto-->>Meta: key provider plugin
    Meta->>KP: createKeyProvider(cryptoMetadata)
    KP-->>Meta: MasterKeyProvider
    Meta->>MKP: decryptKey(wrappedFooterKey)
    MKP-->>Meta: raw footerKey
    Meta->>Bridge: open/read encrypted Parquet with raw footerKey
    Bridge->>Rust: FileDecryptionProperties.builder(footerKey)
    Rust->>File: decrypted metadata and row groups
    Rust-->>Query: batches / reader results
    Meta->>MKP: close()
```

## Lucene Storage Encryption: Directory Creation and Key Setup

```mermaid
sequenceDiagram
    autonumber
    participant OS as OpenSearch index store
    participant Plugin as CryptoDirectoryPlugin
    participant Factory as CryptoDirectoryFactory
    participant Registry as ShardKeyResolverRegistry
    participant Resolver as DefaultKeyResolver
    participant KP as MasterKeyProvider
    participant Cache as EncryptionMetadataCache
    participant Dir as Crypto Directory

    OS->>Plugin: getDirectoryFactories()
    Plugin-->>OS: "cryptofs" -> CryptoDirectoryFactory
    OS->>Factory: newDirectory(indexSettings, shardPath)
    Factory->>Factory: getKeyProvider(indexSettings)
    Factory->>Factory: handleResizeOperation(indexSettings, indexDirectory)
    Factory->>Registry: getOrCreateResolver(indexUuid, indexDirectory, provider, keyProvider, shardId, indexName)
    Registry->>Resolver: new DefaultKeyResolver(...)
    Resolver->>Resolver: read keyfile
    alt keyfile missing
        Resolver->>KP: generateDataPair()
        KP-->>Resolver: raw data key + encrypted key
        Resolver->>Resolver: write keyfile(encrypted key)
    else keyfile exists
        Resolver->>KP: decryptKey(encrypted key)
        KP-->>Resolver: raw data key
    end
    Factory->>Cache: EncryptionMetadataCacheRegistry.getOrCreateCache(indexUuid, shardId, indexName)
    Factory->>Factory: choose NIO, BufferPool, or Hybrid directory
    Factory-->>OS: CryptoNIOFSDirectory / BufferPoolDirectory / HybridCryptoDirectory
```

## Lucene Storage Encryption: NIO Write Path

```mermaid
sequenceDiagram
    autonumber
    participant Lucene as Lucene IndexWriter
    participant Dir as CryptoNIOFSDirectory
    participant Out as CryptoOutputStreamIndexOutput
    participant Resolver as KeyResolver
    participant HKDF as HkdfKeyDerivation
    participant Cipher as OpenSslNativeCipher
    participant Footer as EncryptionFooter
    participant Disk as Lucene file on disk
    participant MetaCache as EncryptionMetadataCache

    Lucene->>Dir: createOutput(name, context)
    alt segments_N or .si
        Dir-->>Lucene: parent NIOFS output
    else encrypted file
        Dir->>Out: new CryptoOutputStreamIndexOutput(...)
        Out->>Footer: generateNew(frameSize, algorithmId)
        Out->>Resolver: getDataKey()
        Resolver-->>Out: index/shard data key
        Out->>HKDF: deriveFileKey(masterKey, footer.messageId)
        HKDF-->>Out: file key
        Out->>Cipher: initGCMCipher(fileKey, frameIV)
        Lucene->>Out: write bytes
        loop chunks and frames
            Out->>Cipher: encryptUpdate(plaintext)
            Cipher-->>Out: ciphertext
            Out->>Disk: write ciphertext
            Out->>Cipher: finalize frame when needed
            Cipher-->>Footer: GCM tag
        end
        Lucene->>Out: close()
        Out->>Cipher: finalize current frame
        Out->>Footer: serialize(fileKey)
        Out->>Disk: append OSEF footer/trailer
        Out->>MetaCache: cache footer + derived file key
    end
```

## Lucene Storage Encryption: NIO Read Path

```mermaid
sequenceDiagram
    autonumber
    participant Lucene as Lucene reader/searcher
    participant Dir as CryptoNIOFSDirectory
    participant In as CryptoBufferedIndexInput
    participant Resolver as KeyResolver
    participant Footer as EncryptionFooter
    participant HKDF as HkdfKeyDerivation
    participant MetaCache as EncryptionMetadataCache
    participant Cipher as AES/CTR cipher
    participant Disk as Lucene file on disk

    Lucene->>Dir: openInput(name, context)
    alt segments_N or .si
        Dir-->>Lucene: parent NIOFS input
    else encrypted file
        Dir->>In: new CryptoBufferedIndexInput(...)
        In->>Resolver: getDataKey()
        Resolver-->>In: master/data key
        In->>Footer: readViaFileChannel(path, channel, masterKey, cache)
        Footer->>Disk: read OSEF footer/trailer
        Footer->>HKDF: deriveFileKey(masterKey, messageId)
        Footer->>Footer: verify footer auth tag
        Footer->>MetaCache: cache footer + file key
        Lucene->>In: readInternal(buffer)
        loop requested chunks
            In->>Disk: read ciphertext at position
            In->>MetaCache: get frame IV / metadata
            In->>Cipher: init decrypt cipher(fileKey, frameIV)
            Cipher-->>In: plaintext bytes
            In-->>Lucene: plaintext
        end
    end
```

## Lucene Storage Encryption: BufferPool / Direct I/O Read Path

```mermaid
sequenceDiagram
    autonumber
    participant Lucene as Lucene reader/searcher
    participant Dir as BufferPoolDirectory
    participant Input as CachedMemorySegmentIndexInput
    participant Tiny as BlockSlotTinyCache
    participant Cache as CaffeineBlockCache
    participant Loader as CryptoDirectIOBlockLoader
    participant Resolver as KeyResolver
    participant Footer as EncryptionFooter
    participant Decryptor as MemorySegmentDecryptor
    participant Disk as Encrypted file

    Lucene->>Dir: openInput(name, context)
    Dir->>Footer: read footer to calculate logical content length
    Dir->>Input: newInstance(path, contentLength, blockCache, readAhead)
    Lucene->>Input: readByte/readBytes/readLong/...
    Input->>Tiny: acquire block for offset
    alt cache hit
        Tiny-->>Input: pinned plaintext memory segment
    else cache miss
        Tiny->>Cache: get(blockKey)
        Cache->>Loader: load(file, blockOffset, blockCount)
        Loader->>Disk: directIOReadAligned(ciphertext)
        Loader->>Resolver: getDataKey()
        Resolver-->>Loader: master/data key
        Loader->>Footer: readViaFileChannel(...)
        Loader->>Decryptor: decryptInPlaceFrameBased(...)
        Decryptor-->>Loader: plaintext memory segment
        Loader-->>Cache: decrypted RefCountedMemorySegment
        Cache-->>Tiny: pinned plaintext memory segment
    end
    Input-->>Lucene: plaintext bytes
```

## Component Class Structure: Parquet PME

```mermaid
classDiagram
    class ParquetDataFormatPlugin {
        +indexingEngine(config, checksumStrategy)
        +getFormatDescriptors(indexSettings, registry)
    }

    class ParquetIndexingEngine {
        -ParquetModularEncryptionConfig encryptionConfig
        +createWriter(writerGeneration)
        -initializeEncryption(indexSettings)
    }

    class ParquetModularEncryptionConfig {
        -String kmsInstanceId
        -String kmsInstanceType
        -String kmsKeyArn
        -String kmsEncryptionContext
        -footerKey
        -wrappedFooterKey
        +footerKey()
        +wrappedFooterKey()
    }

    class ParquetWriter {
        -VSRManager vsrManager
        -ParquetModularEncryptionConfig encryptionConfig
        +addDoc(doc)
        +flush()
        +sync()
    }

    class VSRManager {
        -NativeParquetWriter writer
        -VSRPool vsrPool
        -ParquetModularEncryptionConfig encryptionConfig
        +addDocument(doc)
        +flush()
        +sync()
    }

    class NativeParquetWriter {
        -String filePath
        +write(arrayAddress, schemaAddress)
        +flush()
        +sync()
    }

    class RustBridge {
        +createWriter(file, schemaAddress, encryptionConfig)
        +getFileMetadata(file, encryptionConfig)
        +getDecryptedNumRows(file, encryptionConfig)
    }

    class RustFFM {
        +parquet_create_writer()
        +parquet_write()
        +parquet_finalize_writer()
        +parquet_get_file_metadata_decrypted()
        +parquet_get_decrypted_num_rows()
    }

    class RustNativeParquetWriter {
        +create_writer(filename, schema, encryption_options)
        +write_data(filename, array, schema)
        +finalize_writer(filename)
        +get_file_metadata_decrypted(filename, footer_key)
    }

    class ParquetEncryptionOptions {
        +String kms_instance_id
        +String kms_instance_type
        +String kms_key_arn
        +String kms_encryption_context
        +footer_key
        +wrapped_footer_key
    }

    ParquetDataFormatPlugin --> ParquetIndexingEngine
    ParquetIndexingEngine --> ParquetModularEncryptionConfig
    ParquetIndexingEngine --> ParquetWriter
    ParquetWriter --> VSRManager
    VSRManager --> NativeParquetWriter
    NativeParquetWriter --> RustBridge
    RustBridge --> RustFFM
    RustFFM --> RustNativeParquetWriter
    RustNativeParquetWriter --> ParquetEncryptionOptions
```

## Component Class Structure: Lucene Storage Encryption

```mermaid
classDiagram
    class CryptoDirectoryPlugin {
        +getDirectoryFactories()
        +getEngineFactory(indexSettings)
        +createComponents()
    }

    class CryptoDirectoryFactory {
        +newDirectory(indexSettings, shardPath)
        +newFSDirectory(location, lockFactory, indexSettings)
        -getKeyProvider(indexSettings)
        -createCryptoBufferPoolFSDirectory()
    }

    class CryptoNIOFSDirectory {
        +openInput(name, context)
        +createOutput(name, context)
        +fileLength(name)
    }

    class BufferPoolDirectory {
        +openInput(name, context)
        +createOutput(name, context)
        -calculateContentLengthWithValidation(file, rawSize)
    }

    class HybridCryptoDirectory {
        -BufferPoolDirectory bufferPoolDirectory
        -nioExtensions
        +openInput(name, context)
        +createOutput(name, context)
    }

    class DefaultKeyResolver {
        -Directory directory
        -MasterKeyProvider keyProvider
        +getDataKey()
        -initialize(shardId)
        -loadKeyFromMasterKeyProvider()
    }

    class ShardKeyResolverRegistry {
        +getOrCreateResolver()
        +getResolver(indexUuid, shardId, indexName)
        +removeResolver()
    }

    class NodeLevelKeyCache {
        +get(indexUuid, shardId, indexName)
        +refreshKey(indexUuid, shardId, indexName)
        +evict(indexUuid, shardId, indexName)
    }

    class CryptoOutputStreamIndexOutput {
        +writeBytes()
        +close()
    }

    class CryptoBufferedIndexInput {
        +readInternal(buffer)
        +slice(desc, offset, length)
        +length()
    }

    class BufferIOWithCaching {
        +writeBytes()
        +close()
    }

    class CachedMemorySegmentIndexInput {
        +readByte()
        +readBytes()
        +slice()
    }

    class CryptoDirectIOBlockLoader {
        +load(filePath, startOffset, blockCount, timeout)
    }

    class EncryptionFooter {
        +generateNew(frameSize, algorithmId)
        +serialize(filePath, fileKey)
        +readViaFileChannel(path, channel, masterKey, cache)
    }

    class EncryptionMetadataCache {
        +getOrLoadMetadata(path, footer, masterKey)
        +getFooter(path)
        +getFrameIv(path, frameNumber)
        +invalidateFile(path)
    }

    class HkdfKeyDerivation {
        +deriveFileKey(masterKey, messageId)
        +deriveKey(masterKey, messageId, context, keyLength)
    }

    CryptoDirectoryPlugin --> CryptoDirectoryFactory
    CryptoDirectoryFactory --> ShardKeyResolverRegistry
    ShardKeyResolverRegistry --> DefaultKeyResolver
    DefaultKeyResolver --> NodeLevelKeyCache
    CryptoDirectoryFactory --> CryptoNIOFSDirectory
    CryptoDirectoryFactory --> BufferPoolDirectory
    CryptoDirectoryFactory --> HybridCryptoDirectory
    CryptoNIOFSDirectory --> CryptoOutputStreamIndexOutput
    CryptoNIOFSDirectory --> CryptoBufferedIndexInput
    BufferPoolDirectory --> BufferIOWithCaching
    BufferPoolDirectory --> CachedMemorySegmentIndexInput
    CachedMemorySegmentIndexInput --> CryptoDirectIOBlockLoader
    CryptoOutputStreamIndexOutput --> EncryptionFooter
    BufferIOWithCaching --> EncryptionFooter
    CryptoBufferedIndexInput --> EncryptionFooter
    CryptoDirectIOBlockLoader --> EncryptionFooter
    EncryptionFooter --> HkdfKeyDerivation
    EncryptionFooter --> EncryptionMetadataCache
```

## Key Material Object Diagram

```mermaid
flowchart LR
    subgraph ParquetPME["Parquet PME current prototype"]
        PIndexSettings["index.store.crypto.* settings"]
        PCryptoMetadata["CryptoMetadata.fromIndexSettings"]
        PMKP["MasterKeyProvider"]
        PPair["DataKeyPair"]
        PConfig["ParquetModularEncryptionConfig"]
        PFile["Encrypted Parquet file"]
        PCommit["WriterFileSet PME metadata"]

        PIndexSettings --> PCryptoMetadata
        PCryptoMetadata --> PMKP
        PMKP --> PPair
        PPair -->|"rawKey"| PConfig
        PPair -->|"encryptedKey"| PConfig
        PConfig -->|"footerKey"| PFile
        PConfig -->|"wrappedFooterKey"| PFile
        PConfig -->|"wrappedFooterKey base64"| PCommit
    end

    subgraph LuceneEncryption["Lucene storage encryption"]
        LIndexSettings["index.store.crypto.* settings"]
        LKP["MasterKeyProvider"]
        LPair["DataKeyPair"]
        LKeyfile["index-level keyfile"]
        LResolver["DefaultKeyResolver"]
        LCache["NodeLevelKeyCache"]
        LFooter["per-file OSEF footer with messageId"]
        LHKDF["HKDF derived file key"]
        LFile["Encrypted Lucene file"]

        LIndexSettings --> LKP
        LKP --> LPair
        LPair -->|"encryptedKey"| LKeyfile
        LKeyfile --> LResolver
        LResolver --> LCache
        LCache -->|"master/data key"| LHKDF
        LFooter -->|"messageId"| LHKDF
        LHKDF --> LFile
        LFooter --> LFile
    end
```
