# backpak

Some pipe dream of a backup system:

- Deduplication and content-addressed storage, a la [restic](https://restic.net/)

- Compression, because plenty of things are worth compressing and Zstd is fast.

- Don't roll your own crypto - support for GPG or other external crypto of your choice.
