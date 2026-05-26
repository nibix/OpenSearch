# PME Key Visualization

This document visualizes the simplified v1 Parquet PME key model.

The mental model is:

```text
Parquet file -> data_key_id + message_id
data_key_id = default -> index-level keyfile
MasterKeyProvider unwraps keyfile bytes once
data key + message_id -> derived Parquet file key
derived key decrypts the Parquet file
```

## Key Hierarchy

```mermaid
flowchart TD
    KMS["KMS key<br/>Long-lived wrapping key<br/>Not exported to OpenSearch"]
    KEYFILE["index-level keyfile<br/>provider-wrapped data key bytes<br/>same Lucene location"]
    DATAKEY["dataKey<br/>Plaintext 32-byte data key<br/>Hydrated through MasterKeyProvider and cached"]

    FILEMETA["PME footer_key_metadata JSON<br/>version<br/>data_key_id = default<br/>message_id"]
    MSG["message_id<br/>Random 16 bytes per Parquet file"]
    CTX["context<br/>opensearch/parquet-pme/footer-key/v1"]
    AAD["AAD prefix bytes<br/>domain + version + data_key_id + message_id"]

    DERIVE["HMAC-SHA384 derivation<br/>PRK = HMAC(dataKey, message_id)<br/>T1 = HMAC(PRK, context || 0x01)<br/>fileKey = first 32 bytes"]
    FILEKEY["pmeFooterKey<br/>Derived 32-byte Parquet file key"]
    PARQUET["Encrypted Parquet file<br/>PME footer and pages"]

    KMS --> KEYFILE
    KEYFILE -->|"decryptKey once per data-key cache miss"| DATAKEY
    FILEMETA --> MSG
    FILEMETA --> AAD
    DATAKEY --> DERIVE
    MSG --> DERIVE
    CTX --> DERIVE
    DERIVE --> FILEKEY
    FILEKEY --> PARQUET
    FILEMETA --> PARQUET
    AAD --> PARQUET
```

## Footer Key Metadata JSON

```mermaid
flowchart LR
    subgraph PME["PME footer_key_metadata V1 JSON"]
        VERSION["version<br/>1"]
        DATAID["data_key_id<br/>default"]
        MID["message_id<br/>base64url-no-padding<br/>16 random bytes"]
    end

    DATAID --> LOOKUP["Resolve to index-level keyfile"]
    MID --> DERIVE["Derive this file's PME key"]
```

## AAD Prefix Bytes

```mermaid
flowchart LR
    DLEN["u16_be<br/>domain length = 30"]
    DOMAIN["UTF-8 domain<br/>opensearch/parquet-pme/file/v1"]
    VERSION["u8<br/>version = 1"]
    ILEN["u16_be<br/>data_key_id length = 7"]
    ID["UTF-8 data_key_id<br/>default"]
    MSG["message_id[16]<br/>raw bytes decoded from JSON"]

    DLEN --> DOMAIN --> VERSION --> ILEN --> ID --> MSG
```

## Index-Level Keyfile

```mermaid
flowchart LR
    subgraph OS["OpenSearch index directory"]
        LOCATION[".../indices/{index-uuid}/keyfile"]
        PAYLOAD["raw file payload<br/>encrypted data key bytes"]
    end

    PROVIDER["Lucene-compatible<br/>MasterKeyProvider configuration"] --> PAYLOAD
    LOCATION --> PAYLOAD
    PAYLOAD --> DATAKEY["decryptKey(payload)<br/>dataKey"]
```

## Write Flow

```mermaid
sequenceDiagram
    autonumber
    participant Engine as ParquetIndexingEngine
    participant Store as Index-Level Keyfile
    participant KMS as MasterKeyProvider / KMS
    participant Derive as Key Derivation
    participant Rust as RustBridge / parquet-rs
    participant File as Parquet File

    Engine->>Store: Resolve .../indices/{index-uuid}/keyfile
    alt keyfile does not exist
        Engine->>KMS: generateDataPair()
        KMS-->>Engine: dataKey + encryptedKey
        Engine->>Store: Persist encryptedKey as raw keyfile bytes
    else keyfile exists
        Store-->>Engine: encryptedKey bytes
        Engine->>KMS: decryptKey(encryptedKey) on cache miss
        KMS-->>Engine: dataKey
    end
    Engine->>Engine: Generate random 16-byte message_id
    Engine->>Derive: dataKey + message_id + fixed v1 context
    Derive-->>Engine: pmeFooterKey
    Engine->>Rust: pmeFooterKey + JSON footer_key_metadata + AAD prefix
    Rust->>File: Write encrypted Parquet file
    Rust->>File: Store footer_key_metadata JSON
    Rust->>File: Use AAD prefix for PME authentication
```

## Read Flow

```mermaid
sequenceDiagram
    autonumber
    participant Reader as Parquet Read Path
    participant File as Parquet File
    participant Store as Index-Level Keyfile
    participant KMS as MasterKeyProvider / KMS
    participant Cache as Data Key Cache
    participant Derive as Key Derivation
    participant Rust as parquet-rs Reader

    Reader->>File: Read PME footer_key_metadata
    File-->>Reader: version + data_key_id + message_id
    Reader->>Reader: Validate version/data_key_id/message_id
    Reader->>Store: Resolve data_key_id = default to keyfile
    Store-->>Reader: encryptedKey bytes
    Reader->>Cache: Get dataKey for index/data_key_id
    alt cache miss
        Reader->>KMS: decryptKey(encryptedKey)
        KMS-->>Reader: dataKey
        Reader->>Cache: Cache dataKey
    else cache hit
        Cache-->>Reader: dataKey
    end
    Reader->>Derive: dataKey + message_id + fixed v1 context
    Derive-->>Reader: pmeFooterKey
    Reader->>Reader: Build AAD prefix from metadata
    Reader->>Rust: FileDecryptionProperties(pmeFooterKey, AAD prefix)
    Rust->>File: Decrypt and validate Parquet footer/pages
```

## Derivation Inputs

```mermaid
flowchart TD
    KEYFILE["index-level keyfile<br/>provider-wrapped data key bytes"]
    DATAKEY["dataKey<br/>Secret<br/>32 bytes<br/>unwrapped from keyfile"]
    MSG["message_id<br/>Public<br/>16 bytes<br/>One per Parquet file"]
    CONTEXT["context<br/>Fixed by version 1<br/>opensearch/parquet-pme/footer-key/v1"]
    VERSION["version<br/>1<br/>Selects this derivation algorithm"]

    DERIVE["Derivation<br/>PRK = HMAC-SHA384(dataKey, message_id)<br/>T1 = HMAC-SHA384(PRK, context || 0x01)<br/>fileKey = first 32 bytes"]
    KEY["pmeFooterKey<br/>Secret<br/>32 bytes"]

    KEYFILE --> DATAKEY
    DATAKEY --> DERIVE
    MSG --> DERIVE
    CONTEXT --> DERIVE
    VERSION --> DERIVE
    DERIVE --> KEY
```

## Lucene Comparison

```mermaid
flowchart TB
    subgraph Lucene["Lucene storage encryption"]
        LKEYFILE["index-level keyfile<br/>wrapped data key"]
        LROOT["data key"]
        LMSG["file footer messageId"]
        LKDF["HMAC-SHA384 derivation<br/>context: file-encryption"]
        LFILE["Lucene file key"]

        LKEYFILE --> LROOT
        LROOT --> LKDF
        LMSG --> LKDF
        LKDF --> LFILE
    end

    subgraph Parquet["Parquet PME v1"]
        PKEYFILE["index-level keyfile<br/>same Lucene location"]
        PMETA["PME footer_key_metadata JSON<br/>data_key_id + message_id"]
        PDATAKEY["data key"]
        PMSG["message_id"]
        PKDF["HMAC-SHA384 derivation<br/>context: opensearch/parquet-pme/footer-key/v1"]
        PFILE["Parquet PME file key"]

        PKEYFILE --> PDATAKEY
        PMETA --> PMSG
        PDATAKEY --> PKDF
        PMSG --> PKDF
        PKDF --> PFILE
    end
```

## What Is Persisted

```mermaid
flowchart LR
    subgraph File["Persisted in each Parquet file"]
        FMETA["footer_key_metadata JSON<br/>version, data_key_id, message_id"]
        CIPHERTEXT["encrypted footer/pages"]
    end

    subgraph OpenSearch["Persisted by OpenSearch"]
        GMETA["index-level keyfile<br/>provider-wrapped data key bytes"]
    end

    subgraph Memory["Only in memory"]
        DATAKEY["dataKey"]
        FILEKEY["pmeFooterKey"]
    end

    FMETA --> CIPHERTEXT
    GMETA --> DATAKEY
    DATAKEY --> FILEKEY
    FILEKEY --> CIPHERTEXT
```
