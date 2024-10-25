# Bup: Encrypted Filesystem Backup Tool

Bup is a Rust library designed to efficiently back up LUKS-encrypted filesystem images.

## How It Works

The backup process:

1. Files are read in fixed-size chunks
2. Multiple chunks are hashed simultaneously using BLAKE3
3. Chunks are stored using their hash as the identifier
4. A metadata blob containing all chunk hashes is stored as the root

The restore process:

1. Retrieves the metadata blob containing chunk hashes
2. Downloads each chunk using its hash
3. Verifies chunk integrity
4. Reassembles the original file
