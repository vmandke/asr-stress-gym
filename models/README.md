# Models

`fetch.sh` lands at M5. It runs at **image build time only** — weights are
baked into each worker image, never downloaded when a container starts
(see [../docs/build-plan.md](../docs/build-plan.md#one-command), "Model
weights"). Nothing under this directory is committed to the repository;
build output is git-ignored (see `../.gitignore`).
