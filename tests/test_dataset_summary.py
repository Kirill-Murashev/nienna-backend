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


def test_multi_regression_smoke() -> None:
    client = TestClient(app)
    response = client.post(
        "/api/v1/nienna/modeling/regression",
        json={
            "object_level": "Регион",
            "year_from": 2020,
            "year_to": 2024,
            "dependent_indicator": {"code": "Y477110374", "transform": "log"},
            "predictor_indicators": [
                {"code": "Y477110108", "transform": "log", "lag_years": 1},
                {"code": "Y477110461", "transform": "log", "lag_years": 0},
            ],
            "include_year_fixed_effects": True,
            "include_object_fixed_effects": True,
            "cluster_by": "object",
            "include_pairwise_interactions": True,
            "event_study_indicator_code": "Y477110108",
            "event_study_max_lag_years": 2,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["observations_count"] > 0
    assert payload["regression"]["observations_count"] > 0
    assert payload["regression"]["coefficients"]
    assert payload["regression"]["interpretation"]["headline"]
    assert payload["include_object_fixed_effects"] is True
    assert payload["regression"]["standard_errors_type"] == "object"
    assert payload["event_study"]["points"]


def test_report_pdf_smoke() -> None:
    client = TestClient(app)
    response = client.post(
        "/api/v1/nienna/report/pdf",
        json={
            "title": "PDF Memo",
            "saved_views_count": 1,
            "explorer_normalization": "raw",
            "dataset_rows": 1969010,
            "cards": [
                {
                    "kind": "indicator",
                    "title": "PDF Test",
                    "subtitle": "2024",
                    "primary": "Primary insight",
                    "secondary": "Secondary insight",
                    "notes": ["note"],
                }
            ],
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/pdf")
    assert response.content.startswith(b"%PDF")
