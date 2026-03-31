from fastapi.testclient import TestClient

from app.main import app


def test_dataset_summary_smoke() -> None:
    client = TestClient(app)
    response = client.get("/api/v1/nienna/dataset")
    assert response.status_code == 200
    payload = response.json()
    assert payload["dataset"] == "data_regions_collection_102_v20260313.parquet"
    assert payload["rows"] > 0
    assert payload["columns_count"] > 0


def test_indicator_search_smoke() -> None:
    client = TestClient(app)
    response = client.get("/api/v1/nienna/indicators/search", params={"query": "население", "limit": 5})
    assert response.status_code == 200
    payload = response.json()
    assert payload["items"]


def test_region_profile_smoke() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/v1/nienna/regions/Москва/profile",
        params={"year": 2024, "benchmark_name": "Российская Федерация"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["overview_cards"]


def test_correlation_lab_smoke() -> None:
    client = TestClient(app)
    response = client.post(
        "/api/v1/nienna/correlation",
        json={
            "year": 2024,
            "object_level": "Регион",
            "x_indicator": {"code": "Y477110108"},
            "y_indicator": {"code": "Y477110374"},
            "x_transform": "log",
            "y_transform": "log",
            "regression_model": "linear",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["transformed_summary"]["observations_count"] > 0
    assert payload["points"]
    assert payload["regression"]["r_squared"] is not None


def test_report_brief_smoke() -> None:
    client = TestClient(app)
    response = client.post(
        "/api/v1/nienna/report/brief",
        json={
            "title": "Smoke Memo",
            "saved_views_count": 2,
            "explorer_normalization": "raw",
            "dataset_rows": 1969010,
            "cards": [
                {
                    "kind": "indicator",
                    "title": "Инвестиции",
                    "subtitle": "2024",
                    "primary": "Высокая концентрация в лидерах",
                    "secondary": "Разрыв между регионами сохраняется",
                    "notes": ["Москва лидирует", "Сырьевые регионы выше среднего"],
                }
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cards_count"] == 1
    assert "Smoke Memo" in payload["markdown"]
