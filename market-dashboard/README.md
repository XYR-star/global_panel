# Market Dashboard

Grafana-based market dashboard for `market.heyrickishere.com`.

## What is here

- `docker-compose.yml`: Grafana + Postgres
- `sync_market_data.py`: fetches market and macro series into Postgres
- `grafana/provisioning/`: datasource + dashboard provisioning
- `deploy/nginx/`: nginx site config
- `deploy/systemd/`: timer-driven sync service

## Default data providers

- Market data: `fredgraph.csv` market and commodity series
- Macro / rates: `fredgraph.csv`

That means the first version can run without paid API keys. The tradeoff is narrower coverage than a paid market feed. If you later want broader or more current market coverage, add an EODHD layer and keep the rest of the stack unchanged.

## Local bootstrap outline

1. Copy `.env.example` to `/etc/market-dashboard.env` and set passwords.
2. Install Docker, Docker Compose v2, Python venv support.
3. Create `.venv` and install `requirements.txt`.
4. `docker compose --env-file /etc/market-dashboard.env up -d`
5. Run the sync once.
6. Install the systemd timer and nginx config.
7. Add DNS for `market.heyrickishere.com` and issue a cert with certbot.
