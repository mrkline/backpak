# File Formats and Implementation Details

## Concepts

Every backup starts by cutting files into content-defined chunks,
roughly 1MB[^1] in size, using the
[FastCDC algorithm](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf).
Chunks are then ID'd by their [SHA-224](https://en.wikipedia.org/wiki/SHA-2) hash.

Next, we need to organize lists of chunks back into their respective files,
and files back into their directories. Let's represent each directory as a *tree*,
where each node is a file made of chunks:
```
"PXL_20240804_202813830.jpg": {
    "chunks": [
      "oo98aq2o7ma75pmgmu6qc40jm8ds5blod7ne3ooendmqe",
      "73rqnbmg905r3sv77eqcpvgjodbsv6m8mon6kdobj8vfq"
    ],
    "metadata": {
      "type": "posix",
      "mode": 33188,
      "size": 1097373,
      "uid": 1000,
      "gid": 100,
      "atime": "2024-08-17T19:38:42.334637269Z",
      "mtime": "2024-08-06T01:40:45.36797951Z"
    }
  }
```

A node can also be a subdirectory, whose ID is the SHA-224 of its serialized tree.
```
"Camera": {
  "tree": "cti2sslfl8i9j3kvvfqkv2bust1pd1oiks0n2nhkg6ecu",
  "metadata": {
    "type": "posix",
    "mode": 16877,
    "uid": 1000,
    "gid": 100,
    "atime": "2024-08-17T08:13:52.026693074Z",
    "mtime": "2024-08-16T07:35:05.949493629Z"
  }
```
Note that we save basic metadata (owners, permissions, etc.)
but omit things we can't easily restore, or which depend on particular filesystems
(inode numbers, change times, extended attributes, etc.).
Backpak focuses on saving your files in a space-efficient format, not trying to make
an exact image of a POSIX filesystem a la `tar` or `rsync`.
Special files like dev nodes and sockets are skipped for this same reason.

## Files

### Packs

Saving each chunk and tree as a separate file would make the backup larger than its source material.
Instead, let's group them into larger files, which we'll call *packs*.
We aim for 100 MB per pack, though compression shenanigans can cause it to overshoot.[^1]

Each pack contains:
1. The magic bytes `MKBAKPAK`
2. The file version number (currently 1)
3. A [Zstandard](https://github.com/facebook/zstd)-compressed stream of either chunks or trees
   (which we'll collectively call *blobs*)
4. A manifest of what's in the pack, as `(blob type, length, ID)` tuples.
5. The manifest length, in bytes, as a 32-bit big-endian integer.
   This lets a reader quickly seek to the manifest.

Since a pack's manifest uniquely identifies all the blobs inside
(and, for what it's worth, the order in which they're stored),
the SHA-224 of the manifest is the pack's ID.

### Indexes

Reading each pack every time to rediscover its contents would be a huge slowdown,
and for cloud-stored repositories, a huge bandwidth suck.
As we make a backup, let's build an *index* of all the packs we've built.
Each index contains:
1. The magic bytes `MKBAKIDX`
2. The file version number (currently 1)
3. A Zstandard-compressed map of each pack's ID to its manifest

We can also use the index for resumable backups!
As we finish each pack, we write a work-in-progress index to disk.
If the backup is interrupted and restarted, we read the WIP index and resume from
wherever the last pack left off.

### Snapshots

After packing all our blobs and writing the index,
the last step of a backup is to upload a *snapshot*.
Each contains:
1. The magic bytes `MKBAKSNP`
2. The file version number (currently 1)
3. A [CBOR](https://cbor.io/) file containing snapshot metadata (author, tags, and time),
   the absolute paths that the snapshot backed up,
   and the root tree of the backup.

We don't bother with compressing snapshots since they're so small.

-----

[^1]: Smaller chunks means better deduplication, but more to keep track of.
      1MB was chosen as a hopefully-reasonable compromise — each gigabyte of chunks
      gives about 30 kilobytes of chunk IDs.

[^2]: It's hard to know how large a compressed stream will be without flushing it,
      and flushing often can hurt the overall compression ratio.
      Backpak tries not to do that, but this means it often overshoots packs' target size.
