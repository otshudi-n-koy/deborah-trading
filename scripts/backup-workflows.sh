#!/bin/bash
docker exec n8n-n8n-1 n8n export:workflow --all --output=/tmp/workflows-export.json
docker cp n8n-n8n-1:/tmp/workflows-export.json /opt/deborah-trading/workflows/workflows-$(date +%Y%m%d).json
cd /opt/deborah-trading
git add workflows/
git commit -m "backup: workflows $(date +%Y-%m-%d)" || true
git push || true
