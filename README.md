# oracle-elt-workflow

Simple Oracle ELT project for portfolio and learning.

Current MVP (working):
- flat file -> external table
- external table -> stage
- stage -> core
- one procedure to run full load
- optional scheduler job

We use one simple environment for now: local `dev/sandbox`.

## Quick start

1. Create Docker volume:
```bash
docker volume create oracle-data
```

2. Run Oracle Free:
```bash
docker rm -f oracle-free 2>/dev/null || true

docker run -d --name oracle-free \
  -p 1521:1521 -p 5500:5500 \
  -e ORACLE_PASSWORD='<ORACLE_PASSWORD>' \
  -v oracle-data:/opt/oracle/oradata \
  -v $(pwd)/extdata:/opt/oracle/extdata \
  gvenzl/oracle-free:latest
```

3. Check logs:
```bash
docker logs -f oracle-free
```

## Main SQL files
- `sql/02_create_external_client_transfers.sql`
- `sql/04_create_load_procedure.sql`
- `sql/05_create_scheduler_job.sql`
- `sql/08_run_load_procedure.sql`

## Project folders
- `sql/` SQL scripts
- `extdata/` input files
- `src/` app/etl code
- `docs/adr/` public decisions

## Notes
- Do not commit real passwords/secrets.
- Internal notes are in `docs/internal/` (not public docs).
