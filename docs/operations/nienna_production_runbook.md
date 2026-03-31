# Nienna Production Runbook

## Канонические точки
- Backend repo on server: `/opt/nienna-backend`
- Public app: `https://app.digitalpm.info`
- Public API prefix: `https://app.digitalpm.info/api/nienna`
- Local backend port: `127.0.0.1:8015`

## Базовые проверки
```bash
cd /opt/nienna-backend
git status --short
git rev-parse HEAD
curl -fsS http://127.0.0.1:8015/healthz
./scripts/smoke_test_production.sh
```

## Deploy backend
```bash
cd /opt/nienna-backend
git pull --ff-only origin main
./scripts/deploy_production.sh
./scripts/smoke_test_production.sh
```

## nginx
Публичный префикс `/api/nienna/` должен проксироваться на `http://127.0.0.1:8015/`.
