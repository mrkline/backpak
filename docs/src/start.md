# Getting Started

Backpak backs files up to a _repository_. Let's make one to back up to a local folder:
```
backpak --repository ~/myrepo init filesystem
```
We could also back up to Backblaze B2, in which case the `-r/--repository` flag just states
where to store the repo's config:
```
backpak -r ~/myrepo.toml \
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
