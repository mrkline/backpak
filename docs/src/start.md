# Getting Started

## Installation

[Get yourself a Rust toolchain](https://rustup.rs/) and run
```
$ cargo install backpak
```

Better packaging to follow.

## Creating a repository

Backpak saves backups in a _repository_. Let's make one to back up to a local folder:
```
backpak --repository ~/myrepo init filesystem
```
If you want to back up to Backblaze B2,
in which case the `-r/--repository` flag just sets the repo's config file:
```
$ backpak -r ~/myrepo.toml \
    init --gpg MY_FAVORITE_GPG_KEY \
    backblaze \
        --key-id "deadbeef"
        --application-key "SOMEBASE64" \
        --bucket "matts-bakpak"
```
By specifying `--gpg`, Backpak will perform a quick check that it can round-trip data with
the given key (using `gpg --encrypt --recipient <KEY>`), then encrypt all files in the repo
with the same command. You can edit the repo [config file](./formats.md) to use a different,
arbitrary command.

More backends to follow.

## Backing up

Once you have a repository, let's make a backup!

Let's pass `-v` to print information as the backup runs.  
(A `--progress` flag is on the top of the TODO list.)
```
$ backpak -v -r ~/myrepo backup ~/src/backpak/src
 INFO Walking {"/home/me/src/backpak/src"} to see what we've got...
 INFO Opening repository /home/me/myrepo
 INFO Building a master index
 INFO Finding a parent snapshot
 INFO Backing up /home/me/src/backpak/src
 INFO    backup /home/me/src/backpak/src/backend/backblaze.rs
 INFO    backup /home/me/src/backpak/src/backend/cache.rs
...
 INFO    backup /home/me/src/backpak/src/ui/snapshots.rs
 INFO    backup /home/me/src/backpak/src/ui/usage.rs
 INFO  finished /home/me/src/backpak/src/ui/
 INFO    backup /home/me/src/backpak/src/ui.rs
 INFO    backup /home/me/src/backpak/src/upload.rs
 INFO  finished /home/me/src/backpak/src/
 INFO 0 B reused
 INFO 276 KB new data (270 KB files, 6 KB metadata)
```
If interrupted, an incomplete `backup` will leave behind a `backpak-wip.index` and a handful
of other files. This allows Backpak to resume where it left off.

You can also:
- Pass multiple paths to `backup`.
- Specify a backup author with `--author` (otherwise the machine's hostname is used).
- Annotate your backup with `--tag`.
- Skip over files and folders (matching regular expressions) with `--skip`.
- Dereference symbolic links with `-L`.
- See what you'd backup with `--dry-run`.
  (Most commands that change the repo have this!)

Your new backup is saved as a _snapshot_. You can view a list of the repository's snapshots with...
`snapshots`:
```
$ backpak -r ~/myrepo snapshots
snapshot ra8oc2tmpmrssils66gobh2d2p656qko7nn4d7cskceim
Author: my-desktop
Date:   Sun Sep 15 2024 23:31:16 US/Pacific
  - /home/me/src/backpak/src
```

## Manipulating snapshots

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
   + src/some-new-
   + src/some-other-new-thingthing
```

## Restoring data

To restore a snapshot,
```
$ backpak -r ~/myrepo restore LAST
```
by default, `restore` doesn't delete anything that wasn't in the snapshot.
If you want to do that:
```
$ backpak -r ~/myrepo restore --delete LAST
- /home/mrkline/src/backpak/src/some-new-thing
- /home/mrkline/src/backpak/src/some-other-new-thing
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

-----

[^1]: If your Git habits die hard, `HEAD`, `HEAD~1`, `HEAD~2`, etc. also work.
