# Backpak: deduplicating backups done simply

Backpak is a backup and archiving program that offers:

- **Content-Addressed Storage:** Files are [split into chunks](https://crates.io/crates/fastcdc)
    based on what's inside them, and each chunk is tracked with a unique ID.
    This gives us some huge advantages:

    1. Only new chunks are added to the backup, so files are deduplicated even if they are moved or
       renamed between backups!

    1. Because chunks are split based on their contents,
       small changes to large files (e.g., disk images) don't cause the entire file to be recopied.

    1. Because IDs are a cryptographic hash ([SHA-224](https://en.wikipedia.org/wiki/SHA-2)),
       they double as verification that the bytes inside haven't rotted.

- **Compression:** In the bad old days, you had to choose between leaving data uncompressed
     or incurring massive slodwons on already-compressed files (videos, ZIP archives, etc.).
     Today, we have [Zstandard](https://github.com/facebook/zstd),
     and it rips through high-entropy data at several gigabytes a second.
     Backpak uses it almost everywhere.

- **Bring Your Own Encryption:** The first rule of crypto club is "don't roll your own crypto" —
    Backpak uses GPG by default, and can be configured to encrypt your data
    with anything else you'd like.

- **Support for multiple backends:** Backpak was designed to support many different
  backup targets, starting with local filesystems (or anything mounted as such, like SSHFS)
  and [Backblaze B2](https://www.backblaze.com/cloud-storage).
  Additional backends like rsync or S3 are planned next.

Backpak ships as a simple CLI for Linux and MacOS. Windows support is a work-in-progress.

## Why another backup system?

There's lots of good choices when it comes to open-source backup software!
[Restic](https://restic.net/) and [BorgBackup](https://www.borgbackup.org/) are close contenders,
but weren't all the things I wanted in one place.

I hope you find Backpak useful!
