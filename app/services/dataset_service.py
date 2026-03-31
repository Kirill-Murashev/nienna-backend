from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from app.services.themes import PROFILE_HERO_INDICATORS, THEME_DEFINITIONS, theme_codes

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATASET_PATH = PROJECT_ROOT / "data" / "normalized" / "rosstat" / "data_regions_collection_102_v20260313.parquet"

MISSING_VALUE_SENTINELS = (-99999999.0, -77777777.0)


class DatasetService:
    def __init__(self, dataset_path: Path) -> None:
        self.dataset_path = dataset_path
        self.preview_columns = self._build_preview_columns()

    def _build_preview_columns(self) -> list[dict[str, Any]]:
        parquet_file = pq.ParquetFile(self.dataset_path)
        table = parquet_file.read()
        preview_columns: list[dict[str, Any]] = []
        for name in table.column_names[:20]:
            column = table.column(name)
            preview_columns.append(
                {
                    "name": name,
                    "type": str(column.type),
                    "null_count": int(column.null_count),
                }
            )
        return preview_columns

    def _query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        connection = duckdb.connect()
        dataset_path = str(self.dataset_path).replace("'", "''")
        try:
            connection.execute("PRAGMA threads=4")
            connection.execute(
                f"CREATE TEMP VIEW observations AS SELECT * FROM parquet_scan('{dataset_path}')"
            )
            result = connection.execute(sql, params or []).fetchdf()
            return result.to_dict(orient="records")
        finally:
            connection.close()

    def _query_one(self, sql: str, params: list[Any] | None = None) -> dict[str, Any]:
        rows = self._query(sql, params)
        return rows[0] if rows else {}

    def get_dataset_summary(self) -> dict[str, Any]:
        summary = self._query_one(
            """
            SELECT
              count(*) AS rows,
              count(DISTINCT indicator_code) AS indicators_count,
              count(DISTINCT section) AS sections_count,
              min(year) AS year_min,
              max(year) AS year_max,
              count(DISTINCT source) AS sources_count
            FROM observations
            """
        )
        sample_records = self._query(
            """
            SELECT *
            FROM observations
            LIMIT 5
            """
        )
        return {
            "dataset": self.dataset_path.name,
            "path": str(self.dataset_path.relative_to(PROJECT_ROOT)),
            "file_size_bytes": self.dataset_path.stat().st_size,
            "rows": summary["rows"],
            "columns_count": 14,
            "columns": [
                "section",
                "indicator_code",
                "indicator_name",
                "subsection",
                "object_name",
                "object_level",
                "object_oktmo",
                "object_okato",
                "year",
                "indicator_value",
                "indicator_unit",
                "comment",
                "source",
                "version_date",
            ],
            "preview_columns": self.preview_columns,
            "region_column_candidates": ["object_name", "object_level", "object_oktmo", "object_okato"],
            "sample_records": sample_records,
            "indicators_count": summary["indicators_count"],
            "sections_count": summary["sections_count"],
            "year_range": {"from": summary["year_min"], "to": summary["year_max"]},
            "sources_count": summary["sources_count"],
        }

    def get_filters_meta(self) -> dict[str, Any]:
        sections = self._query(
            """
            SELECT
              section,
              count(DISTINCT indicator_code) AS indicators_count,
              count(*) AS observations_count
            FROM observations
            GROUP BY 1
            ORDER BY observations_count DESC, section
            """
        )
        years = self._query(
            """
            SELECT
              year,
              count(*) AS observations_count,
              count(DISTINCT indicator_code) AS indicators_count
            FROM observations
            WHERE object_level = 'Регион'
            GROUP BY 1
            ORDER BY year
            """
        )
        regions = self._query(
            """
            SELECT object_name
            FROM observations
            WHERE object_level = 'Регион'
            GROUP BY 1
            ORDER BY object_name
            """
        )
        districts = self._query(
            """
            SELECT object_name
            FROM observations
            WHERE object_level = 'Федеральный округ'
            GROUP BY 1
            ORDER BY object_name
            """
        )
        units = self._query(
            """
            SELECT indicator_unit, count(*) AS observations_count
            FROM observations
            GROUP BY 1
            ORDER BY observations_count DESC, indicator_unit
            LIMIT 40
            """
        )
        return {
            "themes": THEME_DEFINITIONS,
            "sections": sections,
            "years": years,
            "regions": [row["object_name"] for row in regions],
            "federal_districts": [row["object_name"] for row in districts],
            "object_levels": ["Регион", "Федеральный округ", "Страна"],
            "top_units": units,
        }

    def search_indicators(
        self,
        query: str | None,
        section: str | None,
        theme_id: str | None,
        limit: int,
    ) -> dict[str, Any]:
        filters: list[str] = []
        params: list[Any] = []

        if query:
            filters.append("(lower(indicator_name) LIKE ? OR lower(section) LIKE ?)")
            like = f"%{query.lower()}%"
            params.extend([like, like])
        if section:
            filters.append("section = ?")
            params.append(section)
        if theme_id:
            codes = theme_codes(theme_id)
            if codes:
                filters.append(f"indicator_code IN ({', '.join(['?'] * len(codes))})")
                params.extend(codes)

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)

        items = self._query(
            f"""
            SELECT
              indicator_code,
              indicator_name,
              section,
              min(year) AS year_min,
              max(year) AS year_max,
              count(DISTINCT CASE WHEN subsection <> 'CD' THEN subsection END) AS subsection_count,
              count(DISTINCT source) AS source_count,
              count(DISTINCT CASE WHEN indicator_unit <> 'ND' THEN indicator_unit END) AS unit_count,
              count(*) AS observations_count
            FROM observations
            {where_sql}
            GROUP BY 1, 2, 3
            ORDER BY observations_count DESC, indicator_name
            LIMIT ?
            """,
            params,
        )

        return {"items": items}

    def get_indicator_detail(self, indicator_code: str) -> dict[str, Any]:
        indicator = self._query_one(
            """
            SELECT
              indicator_code,
              indicator_name,
              section,
              min(year) AS year_min,
              max(year) AS year_max,
              count(*) AS observations_count,
              count(DISTINCT object_name) AS objects_count,
              count(DISTINCT source) AS source_count
            FROM observations
            WHERE indicator_code = ?
            GROUP BY 1, 2, 3
            """,
            [indicator_code],
        )
        if not indicator:
            return {}

        subsections = self._query(
            """
            SELECT subsection, count(*) AS observations_count
            FROM observations
            WHERE indicator_code = ? AND subsection <> 'CD'
            GROUP BY 1
            ORDER BY observations_count DESC, subsection
            """,
            [indicator_code],
        )
        units = self._query(
            """
            SELECT indicator_unit, count(*) AS observations_count
            FROM observations
            WHERE indicator_code = ? AND indicator_unit <> 'ND'
            GROUP BY 1
            ORDER BY observations_count DESC, indicator_unit
            """,
            [indicator_code],
        )
        sources = self._query(
            """
            SELECT source, count(*) AS observations_count
            FROM observations
            WHERE indicator_code = ?
            GROUP BY 1
            ORDER BY observations_count DESC, source
            """,
            [indicator_code],
        )
        availability = self._query(
            """
            SELECT
              year,
              count(*) AS observations_count,
              count(DISTINCT CASE WHEN object_level = 'Регион' THEN object_name END) AS regions_count
            FROM observations
            WHERE indicator_code = ?
            GROUP BY 1
            ORDER BY year
            """,
            [indicator_code],
        )
        latest_year = max(row["year"] for row in availability) if availability else None
        return {
            **indicator,
            "subsections": subsections,
            "units": units,
            "sources": sources,
            "availability": availability,
            "latest_year": latest_year,
        }

    def get_indicator_snapshot(
        self,
        indicator_code: str,
        year: int | None,
        subsection: str | None,
        object_level: str,
    ) -> dict[str, Any]:
        if year is None:
            row = self._query_one(
                """
                SELECT max(year) AS latest_year
                FROM observations
                WHERE indicator_code = ?
                """,
                [indicator_code],
            )
            year = row["latest_year"]

        filters = ["indicator_code = ?", "year = ?", "object_level = ?"]
        params: list[Any] = [indicator_code, year, object_level]
        if subsection:
            filters.append("subsection = ?")
            params.append(subsection)

        data = self._query(
            f"""
            SELECT
              object_name,
              indicator_value,
              indicator_unit,
              comment,
              source
            FROM observations
            WHERE {' AND '.join(filters)}
              AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            ORDER BY indicator_value DESC
            """,
            params,
        )
        summary = self._query_one(
            f"""
            SELECT
              count(*) AS observations_count,
              avg(indicator_value) AS avg_value,
              min(indicator_value) AS min_value,
              max(indicator_value) AS max_value
            FROM observations
            WHERE {' AND '.join(filters)}
              AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            """,
            params,
        )
        return {
            "year": year,
            "object_level": object_level,
            "summary": summary,
            "items": data,
            "top": data[:10],
            "bottom": list(reversed(data[-10:])),
        }

    def get_indicator_series(
        self,
        indicator_code: str,
        object_names: list[str],
        subsection: str | None,
        object_level: str | None,
        year_from: int | None,
        year_to: int | None,
    ) -> dict[str, Any]:
        filters = ["indicator_code = ?"]
        params: list[Any] = [indicator_code]
        if object_level is not None:
            filters.append("object_level = ?")
            params.append(object_level)
        if object_names:
            filters.append(f"object_name IN ({', '.join(['?'] * len(object_names))})")
            params.extend(object_names)
        if subsection:
            filters.append("subsection = ?")
            params.append(subsection)
        if year_from is not None:
            filters.append("year >= ?")
            params.append(year_from)
        if year_to is not None:
            filters.append("year <= ?")
            params.append(year_to)

        items = self._query(
            f"""
            SELECT
              object_name,
              year,
              indicator_value,
              indicator_unit,
              source
            FROM observations
            WHERE {' AND '.join(filters)}
              AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            ORDER BY year, object_name
            """,
            params,
        )
        return {"items": items}

    def get_region_profile(
        self,
        region_name: str,
        year: int,
        benchmark_name: str,
    ) -> dict[str, Any]:
        overview_cards: list[dict[str, Any]] = []
        for item in PROFILE_HERO_INDICATORS:
            region_value = self._query_one(
                """
                SELECT
                  indicator_name,
                  subsection,
                  indicator_value,
                  indicator_unit,
                  year,
                  source
                FROM observations
                WHERE indicator_code = ?
                  AND object_name = ?
                  AND year <= ?
                  AND (? IS NULL OR subsection = ?)
                  AND indicator_value NOT IN (-99999999, -77777777)
                ORDER BY year DESC
                LIMIT 1
                """,
                [item["code"], region_name, year, item.get("subsection"), item.get("subsection")],
            )
            benchmark_value = self._query_one(
                """
                SELECT
                  indicator_value,
                  year
                FROM observations
                WHERE indicator_code = ?
                  AND object_name = ?
                  AND year <= ?
                  AND (? IS NULL OR subsection = ?)
                  AND indicator_value NOT IN (-99999999, -77777777)
                ORDER BY year DESC
                LIMIT 1
                """,
                [item["code"], benchmark_name, year, item.get("subsection"), item.get("subsection")],
            )
            if not region_value:
                continue
            overview_cards.append(
                {
                    "title": item["title"],
                    "indicator_code": item["code"],
                    "subsection": item.get("subsection"),
                    "region_value": region_value["indicator_value"],
                    "region_year": region_value["year"],
                    "benchmark_value": benchmark_value.get("indicator_value") if benchmark_value else None,
                    "benchmark_year": benchmark_value.get("year") if benchmark_value else None,
                    "indicator_unit": region_value["indicator_unit"],
                    "source": region_value["source"],
                }
            )

        theme_blocks: list[dict[str, Any]] = []
        for theme in THEME_DEFINITIONS:
            metrics: list[dict[str, Any]] = []
            for indicator in theme["indicators"]:
                metric = self._query_one(
                    """
                    SELECT
                      indicator_value,
                      indicator_unit,
                      year,
                      source
                    FROM observations
                    WHERE indicator_code = ?
                      AND object_name = ?
                      AND year <= ?
                      AND (? IS NULL OR subsection = ?)
                      AND indicator_value NOT IN (-99999999, -77777777)
                    ORDER BY year DESC
                    LIMIT 1
                    """,
                    [
                        indicator["code"],
                        region_name,
                        year,
                        indicator.get("subsection"),
                        indicator.get("subsection"),
                    ],
                )
                if metric:
                    metrics.append(
                        {
                            "title": indicator["title"],
                            "indicator_code": indicator["code"],
                            "subsection": indicator.get("subsection"),
                            "value": metric["indicator_value"],
                            "year": metric["year"],
                            "unit": metric["indicator_unit"],
                            "source": metric["source"],
                        }
                    )
            theme_blocks.append(
                {
                    "id": theme["id"],
                    "title": theme["title"],
                    "description": theme["description"],
                    "metrics": metrics,
                }
            )

        return {
            "region_name": region_name,
            "year": year,
            "benchmark_name": benchmark_name,
            "overview_cards": overview_cards,
            "theme_blocks": theme_blocks,
        }

    def compare_regions(
        self,
        indicators: list[dict[str, Any]],
        object_names: list[str],
        year: int,
    ) -> dict[str, Any]:
        items: list[dict[str, Any]] = []
        for indicator in indicators:
            if not object_names:
                continue
            rows = self._query(
                f"""
                SELECT
                  object_name,
                  indicator_code,
                  indicator_name,
                  subsection,
                  indicator_value,
                  indicator_unit,
                  source
                FROM observations
                WHERE indicator_code = ?
                  AND object_name IN ({', '.join(['?'] * len(object_names))})
                  AND year = ?
                  AND (? IS NULL OR subsection = ?)
                  AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
                ORDER BY object_name
                """,
                [
                    indicator["code"],
                    *object_names,
                    year,
                    indicator.get("subsection"),
                    indicator.get("subsection"),
                ],
            )
            items.extend(rows)

        first_indicator = indicators[0] if indicators else None
        trend = (
            self.get_indicator_series(
                first_indicator["code"],
                object_names,
                first_indicator.get("subsection"),
                "Регион",
                max(2001, year - 9),
                year,
            )
            if first_indicator
            else {"items": []}
        )

        return {"items": items, "trend": trend["items"], "year": year}

    def get_theme_dashboard(
        self,
        theme_id: str,
        year: int,
        object_name: str,
    ) -> dict[str, Any]:
        theme = next((item for item in THEME_DEFINITIONS if item["id"] == theme_id), None)
        if theme is None:
            return {}

        metrics: list[dict[str, Any]] = []
        for indicator in theme["indicators"]:
            detail = self._query_one(
                """
                SELECT
                  indicator_value,
                  indicator_unit,
                  year,
                  source
                FROM observations
                WHERE indicator_code = ?
                  AND object_name = ?
                  AND year <= ?
                  AND (? IS NULL OR subsection = ?)
                  AND indicator_value NOT IN (-99999999, -77777777)
                ORDER BY year DESC
                LIMIT 1
                """,
                [
                    indicator["code"],
                    object_name,
                    year,
                    indicator.get("subsection"),
                    indicator.get("subsection"),
                ],
            )
            ranking = self._query(
                """
                SELECT object_name, indicator_value
                FROM observations
                WHERE indicator_code = ?
                  AND year = ?
                  AND object_level = 'Регион'
                  AND (? IS NULL OR subsection = ?)
                  AND indicator_value NOT IN (-99999999, -77777777)
                ORDER BY indicator_value DESC
                LIMIT 8
                """,
                [indicator["code"], detail.get("year", year), indicator.get("subsection"), indicator.get("subsection")],
            )
            series = self.get_indicator_series(
                indicator["code"],
                [object_name, "Российская Федерация"],
                indicator.get("subsection"),
                None,
                max(2001, year - 9),
                year,
            )
            metrics.append(
                {
                    "title": indicator["title"],
                    "indicator_code": indicator["code"],
                    "subsection": indicator.get("subsection"),
                    "value": detail.get("indicator_value"),
                    "value_year": detail.get("year"),
                    "unit": detail.get("indicator_unit"),
                    "source": detail.get("source"),
                    "top_ranking": ranking,
                    "series": series["items"],
                }
            )

        return {
            "theme": theme,
            "object_name": object_name,
            "year": year,
            "metrics": metrics,
        }


@lru_cache(maxsize=1)
def get_dataset_service() -> DatasetService:
    return DatasetService(DATASET_PATH)
