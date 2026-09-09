# Registry indexer
This project implements functionality to track activity in the registry contract.
A Goldsky pipeline filters on-chain events and pushes parsed records into
Postgres; `fly-app/` (an Actix-web service deployed on Fly.io) reads from
Postgres and serves the registry API to clients.

## Per-network configuration (testnet & mainnet)
Goldsky pipeline definitions are checked in as templates
(`goldsky/v1/index.template.yaml`, `goldsky/archive/index.template.yaml`)
with `${VAR}` placeholders. Per-network values (dataset prefix, start
ledger, root registry contract, hosted-Postgres secret name) live in
`goldsky/networks/<network>.env`. Render concrete pipeline definitions
with:

```sh
./goldsky/scripts/render.sh testnet   # or: mainnet
```

which writes self-contained pipeline dirs under `goldsky/rendered/<network>/`
(gitignored — regenerate, never edit). All other scripts take those
rendered dirs. Deploy the **archive pipeline first**: the v1 pipeline's
`post_init.sql` creates views over `archive.uploads` / `archive.deploys` /
`archive.upgrades`, so on a fresh database it can only succeed once the
archive pipeline has created those tables.

```sh
DATABASE_URL=... ./goldsky/scripts/redeploy.sh goldsky/rendered/testnet/archive
DATABASE_URL=... ./goldsky/scripts/redeploy.sh --number-of-initial-subregistries 7 goldsky/rendered/testnet/v1
```

Each network must use its own Postgres database (pipelines write to the
fixed `v1`/`archive` schemas, so sharing a database would collide) and
its own Fly app for the HTTP API: `fly-app/fly.toml` is the testnet app,
`fly-app/fly_mainnet.toml` the mainnet one
(`fly deploy -c fly_mainnet.toml`).

## Deployment

1. Apply relevant pipeline changes

```sh
./goldsky/scripts/render.sh testnet # or mainnet
./goldsky/scripts/turbo.sh apply goldsky/rendered/testnet/v1/index.yaml # or relevant rendered file
```

`turbo apply` swaps in the new pipeline definition without pausing it,
dropping tables, or resetting its checkpoint.

Use `redeploy.sh` (or the manual "Redeploy Goldsky Pipeline"
GitHub Action) _only_ when the change needs history reprocessed: a
new/changed sink column, a lowered `start_at`, or recovering from the
dynamic-table race described in that script's comments.

2. Update the database with psql

If you made any database changes (likely), apply the database schema.

```sh
psql "$DATABASE_URL" -f goldsky/v1/post_init.sql
```

3. Ship the API

Testnet auto-deploys on merge to `main`. To deploy mainnet, use
the [Fly CLI](https://fly.io/docs/flyctl/install/):

```sh
fly deploy -c fly_mainnet.toml
```

## Goldsky-first approach
Goldsky-first approach uses the rendered `goldsky/rendered/<network>/v1/index.yaml` configuration file as Goldsky pipeline configuration. 
It first filters all events that belong to registry contract, then stores raw events (as a backup data).
Finally, deploy/publish event JSONs are being parsed via SQL transformer and pushed into Postgres.
Note: if there are any migration or changes in the events schema, events would need to be re-processed 
from the first snapshot (tables need to be dropped for re-processing)
Alternatively, tables could be manually migrated after a necessary pipeline change, 
and data would need to be backfilled.

## Serving
`fly-app/` reads from the `v1`/`archive` schemas populated by the pipeline
and serves the registry API. It doesn't do any data processing itself —
just reads what the pipeline has already written. See `fly-app/fly.toml`
/ `fly-app/fly_mainnet.toml` for the per-network deploy config.
