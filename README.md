# Bup: Encrypted Filesystem Backup Tool

Bup is a Rust library designed to efficiently back up LUKS-encrypted filesystem images. It's particularly useful for backing up loop-mounted encrypted containers.

## How it works

1. **Chunking**: The encrypted filesystem image is divided into fixed-size chunks.
2. **Hashing**: Each chunk is hashed using the BLAKE3 algorithm.
3. **Verification**: The backup can be verified against the original data.
