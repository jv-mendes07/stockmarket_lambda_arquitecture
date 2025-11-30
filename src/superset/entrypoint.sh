#!/bin/bash
set -e

# Inicializa o Superset
superset db upgrade
superset init

# Criação idempotente do admin
superset fab create-admin \
  --username admin \
  --password admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@superset.com || true

echo "Starting Supervisor..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/superset.conf
