#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${PORTFOLIO_APP_DIR:-/root/global_panel/market-dashboard}"
ENV_FILE="${PORTFOLIO_ENV_FILE:-$APP_DIR/.env.deploy}"
if [[ ! -f "$ENV_FILE" ]]; then
  ENV_FILE="$APP_DIR/.env"
fi
if [[ ! -f "$ENV_FILE" ]]; then
  echo "No env file found at .env.deploy or .env" >&2
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

BACKUP_ROOT="${PORTFOLIO_BACKUP_ROOT:-/var/lib/portfolio-app/backups}"
UPLOAD_ROOT="${PORTFOLIO_UPLOAD_ROOT:-/var/lib/portfolio-app/uploads}"
RETENTION_DAYS="${PORTFOLIO_BACKUP_RETENTION_DAYS:-30}"
POSTGRES_SERVICE="${PORTFOLIO_POSTGRES_SERVICE:-postgres}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DAY_DIR="$BACKUP_ROOT/$(date -u +%Y/%m/%d)"

mkdir -p "$DAY_DIR"
cd "$APP_DIR"

docker compose --env-file "$ENV_FILE" exec -T "$POSTGRES_SERVICE" \
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc \
  > "$DAY_DIR/portfolio-db-$STAMP.dump"

if [[ -d "$UPLOAD_ROOT" ]]; then
  tar -C "$(dirname "$UPLOAD_ROOT")" -czf "$DAY_DIR/portfolio-uploads-$STAMP.tar.gz" "$(basename "$UPLOAD_ROOT")"
fi

find "$BACKUP_ROOT" -type f -mtime +"$RETENTION_DAYS" -delete
echo "Portfolio backup complete: $DAY_DIR"
