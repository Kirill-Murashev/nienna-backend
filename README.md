# nienna-backend

Бэкенд сервиса `Nienna` для доступа к региональным данным Росстата и их последующего анализа в экосистеме `digitalpm.info`.

## Цель
- Подключать подготовленный parquet-набор региональных данных Росстата.
- Давать API для обзора структуры датасета и базовой аналитики.
- Стать backend-слоем для нового dashboard-модуля `Nienna` в `app.digitalpm.info`.

## Структура
- `app/` - FastAPI-приложение и API.
- `data/` - parquet-файл и будущие производные наборы.
- `deploy/` - nginx-конфиг для проксирования.
- `docs/operations/` - runbook для production.
- `scripts/` - deploy и smoke helper-скрипты.
- `tests/` - smoke-тесты API.

## Данные
- Основной parquet: `data/normalized/rosstat/data_regions_collection_102_v20260313.parquet`

## Быстрый старт
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[test]
uvicorn app.main:app --reload --port 8015
```

## API
- `GET /healthz`
- `GET /api/v1/meta`
- `GET /api/v1/nienna`
- `GET /api/v1/nienna/dataset`

## Production
- Public API prefix: `https://app.digitalpm.info/api/nienna`
- Local backend port: `127.0.0.1:8015`

Smoke test after deploy:
```bash
./scripts/smoke_test_production.sh
```
