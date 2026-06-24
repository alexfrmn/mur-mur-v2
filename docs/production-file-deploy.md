# Production File-Level Deploy

The live `/opt/lifecoach/mur-mur-v2` tree is not a plain git checkout. It keeps
local state, secrets, `node_modules`, and site-specific integration scripts that
must survive repo updates. Production deploys therefore copy an audited file
allowlist from a freshly built git clone instead of replacing the tree.

Use `deploy/production-file-deploy.sh` as the source-controlled deploy contract.
It:

- clones the selected ref (`MURMUR_DEPLOY_REF`, default `main`);
- runs `npm ci`, `npm run build`, `npm run test:unit`, and `npm run test:core`;
- verifies Phase N channel roster/personality markers in built core and wake
  scripts;
- copies only the runtime allowlist into `/opt/lifecoach/mur-mur-v2`;
- backs up overwritten files under `/opt/lifecoach/backups/murmur-file-deploy/`;
- restarts only the Murmur daemons, never the shared NATS broker.

For a build/copy gate without daemon restarts:

```bash
sudo MURMUR_DEPLOY_RESTART=0 deploy/production-file-deploy.sh
```

For production rollout, keep the default canary order unless a runbook says
otherwise:

```bash
sudo deploy/production-file-deploy.sh
```

Channel roster personality binding is a separate runtime flag. Use
`deploy/production-channel-roster-ops.sh`:

```bash
sudo deploy/production-channel-roster-ops.sh status
sudo deploy/production-channel-roster-ops.sh enable
sudo deploy/production-channel-roster-ops.sh disable
```

The ops script edits only each daemon's `agent-config.json` `channelRoster`
section and restarts only the Murmur daemons. It does not touch message DBs,
keys, `.env`, `node_modules`, local ACP glue, or the shared NATS broker.
