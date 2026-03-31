from fastapi.testclient import TestClient

from app.main import app


def test_healthz_smoke() -> None:
    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "nienna-backend"}
