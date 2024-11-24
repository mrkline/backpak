# Getting Started

## Installation

[Get yourself a Rust toolchain](https://rustup.rs/) and run
```
$ cargo install backpak
```

Better packaging to follow.

## Creating a repository

Backpak saves backups in a _repository_. We can make one in a local folder:
```
backpak --repository ~/myrepo init filesystem
```
Or, if you'd like to upload to Backblaze B2,
the `-r/--repository` flag just sets the repo's config file:
```
$ backpak -r ~/myrepo.toml \
    init --gpg MY_FAVORITE_GPG_KEY \
    backblaze \
        --key-id "deadbeef"
        --application-key "SOMEBASE64" \
        --bucket "matts-bakpak"
```
With `--gpg`, Backpak will run a quick check that it can round-trip data
with
```
gpg --encrypt --recipient <KEY>
```
then encrypt all files in the repo using that command.
You can edit the repo [config file](./formats.md) to use a different,
arbitrary command.

More backends to follow.

## Backing up

Let's make a backup!

```
$ backpak -r ~/myrepo backup ~/src/backpak/src
Walking {"/home/me/src/backpak/src"} to see what we've got...
/ 297 KB
Opening repository srctest
Building a master index
Finding a parent snapshot
Running backup...
/ P 17 KB + 7 KB | R 281 KB | Z 8 KB | U 9 KB
I 2 packs indexed
D 20 KB downloaded
/home/me/src/backpak/src

Snaphsot afe4ajdi done
```
We print updates as we go:
- How much we **P**acked into this backup (files + metadata)
- How much we **R**eused from previous backups
- How much **Z**standard ensmallened the data
- How much we **U**ploaded

If interrupted, the incomplete `backup` will leave behind a `backpak-wip.index` and a handful
of other files. This allows Backpak to resume where it left off.

You can also:
- Pass multiple paths to `backup`.
- Specify a backup author with `--author` (otherwise the machine's hostname is used).
- Annotate your backup with `--tag`.
- Skip over files and folders (matching regular expressions) with `--skip`.
- Dereference symbolic links with `-L`.
- See what you'd backup with `--dry-run`.
  (Most commands have this!)

Your new backup is saved as a _snapshot_. You can view a list of the repository's snapshots with...
`snapshots`:
```
$ backpak -r ~/myrepo snapshots
...
snapshot afe4ajdifcgfkghmq2tivqlsjnptvri5inb8inn99k0k2
Author: my-desktop
Date:   Thu Nov 7 2024 22:55:36 US/Pacific

  - /home/me/src/backpak/src
```
By default, we see the snapshot ID, the author, any tags, the date, and the paths backed up.
We can get some additional info by passing more flags:

- `--sizes` will calculate how much data each snapshot adds to the repo.

- `--file-sizes` breaks this down further, showing which files added data,
  sorted largest to smallest.

- `--stat` shows the changes each backup made compared to the previous — what was added,
  removed, etc. (Kinda like `git log --stat`.) Add `--metadata` to see changes to that as well.

## Examining snapshots

Each snapshot can be referenced by a few digits of its ID (enough to be unique),
or relative to the most recent snapshot — `LAST` is the latest,
followed by `LAST~`, then `LAST~2`, `LAST~3`, and so on.[^1]

Using these, we can do some routine things, like list the files in the snapshot:
```
$ backpak -r ~/myrepo ls LAST
src/
src/backend/
src/backend/backblaze.rs
src/backend/cache.rs
...
src/ui/snapshots.rs
src/ui/usage.rs
src/ui.rs
src/upload.rs
```

Or compare the snapshot to whatever's in the directory currently:
```
$ backpak -r ~/myrepo diff ra8o
   + src/some-new-thing
   + src/some-other-new-thing
```

## Restoring data

To restore a snapshot,
```
$ backpak -r ~/myrepo restore LAST
```
by default, `restore` doesn't delete anything. If you want to do that:
```
$ backpak -r ~/myrepo restore --delete LAST
- /home/me/src/backpak/src/some-new-thing
- /home/me/src/backpak/src/some-other-new-thing
```
Additional flags like `--times` and `--permissions` can restore metadata,
and `--output` can restore the snapshot to a different directory than where it came from.

If you'd like to dump an individual file from a snapshot, you can do that too:
```
$ backpak -r ~/myrepo dump LAST src/lib.rs
//! Some big dumb backup system.
//!
//! See the [`backup`] module for an overview and a crappy block diagram.

pub mod backend;
pub mod backup;
pub mod blob;
...
```

## Deleting snapshots

Sometimes you want to remove old snapshots, or you backed up the wrong things.
You can remove a snapshot from your repository with
```
$ backpak -r ~/myrepo forget <ID>
```
This only deletes the snapshot itself, not the data it points to.
(After all, many snapshots can reference the same data!)
To run garbage collection on the repo and remove files that aren't referenced by _any_ snapshot
anymore, run
```
$ backpak -r ~/myrepo prune
```

## Repository health

If you'd like to know how much space a repository is using, try `usage`:
```
$ backpak -r photo-backup.toml usage
2 snapshots, from 2024-08-17T12:39:15 to 2024-08-17T12:57:30
16.48 GB unique data
16.48 GB reused (deduplicated)

2 indexes reference 165 packs

Backblaze usage after zstd compression and gpg:
snapshots: 1 KB
indexes:   448 KB
packs:     16.29 GB
total:     16.29 GB
```

Like any sane backup system, Backpak tries very hard to make sure data is always left in
a consistent state — packs are always uploaded before the index that references them,
which is uploaded before its snapshot, etc.
But if you're the "trust but verify" type:
```
$ backpak -r photo-backup.toml check
```
This reads the indexes and ensures that every pack they mention is present.
`check --read-packs` will go a step further and verify the contents of each pack!
To state the obvious, expect this to take a while since it's reading every byte in the repo.

Read up on [this implementation details](/formats.html) if you're wondering what the hell
an index or a pack is.

## Other commands

- `backpak copy` will copy snapshots between repositories. You can add `--skip` to
  leave files you don't want out of the new one.

- `backpak filter-snapshot` creates a copy of a snapshot _in the same repo_,
  but with certain files skipped. (`--skip` is mandatory!)

- `backpak cat` will print objects in the repo as JSON. It's mostly meant for debugging.

-----

[^1]: If your Git habits die hard, `HEAD`, `HEAD~1`, `HEAD~2`, etc. also work.
