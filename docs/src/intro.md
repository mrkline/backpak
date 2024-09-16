# Backpak: deduplicating backups done simply

Backpak is a backup and archiving program that offers:

- **Content-Addressed Storage:** Files are [split into chunks](https://crates.io/crates/fastcdc)
    based on what's inside them, the each chunks is tracked by a unique ID.
    This gives us some huge advantages:

    1. Only new chunks are added to the backup - files are deduplicated even if they were moved,
       renamed, or were in a previous backup!

    1. Because the chunks are split based on their contents,
       small changes to large files (e.g., disk images) don't cause the entire file to be recopied.

    1. Because IDs are a cryptographic hash ([Sha-224](https://en.wikipedia.org/wiki/SHA-2)),
       they double as verification that the bytes inside haven't rotted.

- **Compression:** In the bad old days, you had to choose between leaving your data uncompressed
     or massive slodwons on already-compressed video.
     Today, we have [Zstandard](https://github.com/facebook/zstd),
     and it rips through high-entropy data at several gigabytes a second.
     Backpak uses it almost everywhere.

- **Bring Your Own Encryption:** The first rule of crypto club is "don't roll your own crypto" —
    Backpak uses GPG for encryption by default, and can be configured to encrypt your data
    with anything else you'd like.

- **Support for multiple backends:** Backpak was designed to support many different
  backup targets, starting with local filesystems (or anything mounted as such, like SSHFS)
  and [Backblaze S2](https://www.backblaze.com/cloud-storage).
  Additional backends like rsync or S3 are obvious next steps.

Backpak ships as a simple CLI for Linux and MacOS — Windows support is a work-in-progress.

## Why another backup system?

There's lots of good choices when it comes to open-source backup software!
[Restic](https://restic.net/) and [BorgBackup](https://www.borgbackup.org/) are close contenders,
but weren't all the things I wanted in one place.

I hope you find Backpak useful!
