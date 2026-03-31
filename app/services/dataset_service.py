from __future__ import annotations

import io
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pyarrow.parquet as pq
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from scipy.stats import t as student_t

from app.services.themes import MODELING_PRESETS, PROFILE_HERO_INDICATORS, THEME_DEFINITIONS, theme_codes

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
            "modeling_presets": MODELING_PRESETS,
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

    def get_correlation_lab(
        self,
        year: int,
        object_level: str,
        x_indicator: dict[str, Any],
        y_indicator: dict[str, Any],
        x_transform: str,
        y_transform: str,
        regression_model: str,
    ) -> dict[str, Any]:
        x_filters = ["indicator_code = ?", "year = ?", "object_level = ?"]
        x_params: list[Any] = [x_indicator["code"], year, object_level]
        if x_indicator.get("subsection"):
            x_filters.append("subsection = ?")
            x_params.append(x_indicator["subsection"])

        y_filters = ["indicator_code = ?", "year = ?", "object_level = ?"]
        y_params: list[Any] = [y_indicator["code"], year, object_level]
        if y_indicator.get("subsection"):
            y_filters.append("subsection = ?")
            y_params.append(y_indicator["subsection"])

        points = self._query(
            f"""
            WITH x AS (
              SELECT object_name, indicator_name, subsection, indicator_value, indicator_unit, source
              FROM observations
              WHERE {' AND '.join(x_filters)}
                AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            ),
            y AS (
              SELECT object_name, indicator_name, subsection, indicator_value, indicator_unit, source
              FROM observations
              WHERE {' AND '.join(y_filters)}
                AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            )
            SELECT
              x.object_name,
              x.indicator_name AS x_indicator_name,
              x.subsection AS x_subsection,
              x.indicator_value AS x_value,
              x.indicator_unit AS x_unit,
              x.source AS x_source,
              y.indicator_name AS y_indicator_name,
              y.subsection AS y_subsection,
              y.indicator_value AS y_value,
              y.indicator_unit AS y_unit,
              y.source AS y_source
            FROM x
            INNER JOIN y USING (object_name)
            ORDER BY x.indicator_value DESC
            """,
            x_params + y_params,
        )

        summary = self._query_one(
            f"""
            WITH x AS (
              SELECT object_name, indicator_value
              FROM observations
              WHERE {' AND '.join(x_filters)}
                AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            ),
            y AS (
              SELECT object_name, indicator_value
              FROM observations
              WHERE {' AND '.join(y_filters)}
                AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            )
            SELECT
              count(*) AS observations_count,
              corr(x.indicator_value, y.indicator_value) AS pearson_correlation,
              avg(x.indicator_value) AS x_avg,
              avg(y.indicator_value) AS y_avg
            FROM x
            INNER JOIN y USING (object_name)
            """,
            x_params + y_params,
        )

        transformed_points: list[dict[str, Any]] = []
        dropped_non_positive = 0
        for item in points:
            x_value = float(item["x_value"])
            y_value = float(item["y_value"])
            if x_transform == "log" and x_value <= 0:
                dropped_non_positive += 1
                continue
            if y_transform == "log" and y_value <= 0:
                dropped_non_positive += 1
                continue
            transformed_x = math.log10(x_value) if x_transform == "log" else x_value
            transformed_y = math.log10(y_value) if y_transform == "log" else y_value
            transformed_points.append(
                {
                    **item,
                    "transformed_x": transformed_x,
                    "transformed_y": transformed_y,
                }
            )

        transformed_summary = self._summarize_transformed_points(transformed_points)
        regression = self._fit_regression(
            points,
            regression_model=regression_model,
            x_transform=x_transform,
            y_transform=y_transform,
        )

        strongest_positive = sorted(
            transformed_points,
            key=lambda item: (item["transformed_x"] - (transformed_summary.get("x_avg") or 0.0))
            * (item["transformed_y"] - (transformed_summary.get("y_avg") or 0.0)),
            reverse=True,
        )[:8]
        strongest_divergence = sorted(
            transformed_points,
            key=lambda item: abs(
                (item["transformed_x"] - (transformed_summary.get("x_avg") or 0.0))
                - (item["transformed_y"] - (transformed_summary.get("y_avg") or 0.0))
            ),
            reverse=True,
        )[:8]

        return {
            "year": year,
            "object_level": object_level,
            "summary": summary,
            "transformed_summary": transformed_summary,
            "x_indicator": x_indicator,
            "y_indicator": y_indicator,
            "x_transform": x_transform,
            "y_transform": y_transform,
            "regression_model": regression_model,
            "points": transformed_points,
            "strongest_positive": strongest_positive,
            "strongest_divergence": strongest_divergence,
            "dropped_non_positive": dropped_non_positive,
            "regression": regression,
        }

    def _summarize_transformed_points(self, points: list[dict[str, Any]]) -> dict[str, Any]:
        if not points:
            return {
                "observations_count": 0,
                "pearson_correlation": None,
                "x_avg": None,
                "y_avg": None,
            }
        xs = [item["transformed_x"] for item in points]
        ys = [item["transformed_y"] for item in points]
        x_avg = sum(xs) / len(xs)
        y_avg = sum(ys) / len(ys)
        sxx = sum((value - x_avg) ** 2 for value in xs)
        syy = sum((value - y_avg) ** 2 for value in ys)
        sxy = sum((x - x_avg) * (y - y_avg) for x, y in zip(xs, ys, strict=False))
        pearson = sxy / math.sqrt(sxx * syy) if sxx > 0 and syy > 0 else None
        return {
            "observations_count": len(points),
            "pearson_correlation": pearson,
            "x_avg": x_avg,
            "y_avg": y_avg,
        }

    def _apply_transform(self, value: float, transform: str) -> float | None:
        if transform == "log":
            if value <= 0:
                return None
            return math.log10(value)
        return value

    def _fit_ols_model(
        self,
        *,
        row_meta: list[dict[str, Any]],
        design_matrix: list[list[float]],
        targets: list[float],
        coefficient_names: list[str],
        equation_label: str,
        interpretation_context: dict[str, Any],
        cluster_by: str = "none",
    ) -> dict[str, Any]:
        if len(targets) < len(coefficient_names) + 1:
            return {
                "model": interpretation_context.get("model", "linear"),
                "observations_count": len(targets),
                "dropped_non_positive": interpretation_context.get("dropped_non_positive", 0),
                "standard_errors_type": cluster_by,
                "coefficients": [],
                "r_squared": None,
                "adjusted_r_squared": None,
                "equation": equation_label if targets else None,
                "residuals": [],
                "interpretation": {
                    "headline": "Недостаточно наблюдений для устойчивой оценки модели.",
                    "bullets": [
                        "Попробуйте сократить число факторов или расширить временной интервал.",
                    ],
                },
            }

        x_matrix = np.asarray(design_matrix, dtype=float)
        y_vector = np.asarray(targets, dtype=float)
        xtx_inv = np.linalg.pinv(x_matrix.T @ x_matrix)
        beta = xtx_inv @ x_matrix.T @ y_vector
        predicted = x_matrix @ beta
        residuals_vector = y_vector - predicted
        n = len(y_vector)
        p = x_matrix.shape[1]
        df = max(n - p, 1)
        sse = float(np.sum(residuals_vector**2))
        mse = sse / max(df, 1)
        sst = float(np.sum((y_vector - np.mean(y_vector)) ** 2))
        r_squared = 1.0 - (sse / sst) if sst > 0 else None
        adjusted_r_squared = (
            1.0 - (1.0 - r_squared) * (n - 1) / df if r_squared is not None and df > 0 else None
        )
        covariance, inference_df = self._estimate_covariance(
            x_matrix=x_matrix,
            residuals_vector=residuals_vector,
            xtx_inv=xtx_inv,
            row_meta=row_meta,
            fallback_df=df,
            cluster_by=cluster_by,
        )
        standard_errors = np.sqrt(np.diag(covariance))
        critical_t = float(student_t.ppf(0.975, inference_df)) if inference_df > 0 else 0.0
        leverage = np.sum((x_matrix @ xtx_inv) * x_matrix, axis=1)

        coefficients: list[dict[str, Any]] = []
        for index, name in enumerate(coefficient_names):
            estimate = float(beta[index])
            se = float(standard_errors[index]) if index < len(standard_errors) else 0.0
            t_stat = estimate / se if se > 0 else None
            p_value = (
                float(2 * (1 - student_t.cdf(abs(t_stat), inference_df)))
                if t_stat is not None and inference_df > 0
                else None
            )
            ci_low = estimate - critical_t * se
            ci_high = estimate + critical_t * se
            coefficients.append(
                {
                    "name": name,
                    "estimate": estimate,
                    "std_error": se,
                    "t_stat": t_stat,
                    "p_value": p_value,
                    "ci_95_low": ci_low,
                    "ci_95_high": ci_high,
                }
            )

        residuals = []
        for item, actual, predicted_value, residual in zip(
            row_meta, y_vector.tolist(), predicted.tolist(), residuals_vector.tolist(), strict=False
        ):
            residuals.append(
                {
                    "object_name": item.get("object_name", "—"),
                    "year": item.get("year"),
                    "predicted_y": predicted_value,
                    "actual_y": actual,
                    "residual": residual,
                }
            )

        influence = []
        for item, actual, predicted_value, residual, h_ii in zip(
            row_meta,
            y_vector.tolist(),
            predicted.tolist(),
            residuals_vector.tolist(),
            leverage.tolist(),
            strict=False,
        ):
            standardized_residual = residual / math.sqrt(max(mse * (1 - h_ii), 1e-12)) if mse > 0 else None
            cooks_distance = (
                ((residual**2) / max(p * mse, 1e-12)) * (h_ii / max((1 - h_ii) ** 2, 1e-12))
                if mse > 0
                else None
            )
            influence.append(
                {
                    "object_name": item.get("object_name", "—"),
                    "year": item.get("year"),
                    "predicted_y": predicted_value,
                    "actual_y": actual,
                    "residual": residual,
                    "standardized_residual": standardized_residual,
                    "leverage": h_ii,
                    "cooks_distance": cooks_distance,
                }
            )

        diagnostics = {
            "rmse": math.sqrt(mse) if mse >= 0 else None,
            "mae": float(np.mean(np.abs(residuals_vector))) if len(residuals_vector) else None,
            "mean_residual": float(np.mean(residuals_vector)) if len(residuals_vector) else None,
            "residual_std": float(np.std(residuals_vector)) if len(residuals_vector) else None,
            "max_abs_residual": float(np.max(np.abs(residuals_vector))) if len(residuals_vector) else None,
            "avg_leverage": float(np.mean(leverage)) if len(leverage) else None,
            "max_leverage": float(np.max(leverage)) if len(leverage) else None,
        }

        return {
            "model": interpretation_context.get("model", "linear"),
            "observations_count": len(targets),
            "dropped_non_positive": interpretation_context.get("dropped_non_positive", 0),
            "standard_errors_type": cluster_by,
            "coefficients": coefficients,
            "r_squared": r_squared,
            "adjusted_r_squared": adjusted_r_squared,
            "equation": equation_label,
            "residuals": sorted(residuals, key=lambda item: abs(item["residual"]), reverse=True)[:12],
            "influence": sorted(
                influence,
                key=lambda item: (
                    abs(item["cooks_distance"]) if item["cooks_distance"] is not None else 0.0,
                    abs(item["standardized_residual"]) if item["standardized_residual"] is not None else 0.0,
                ),
                reverse=True,
            )[:12],
            "diagnostics": diagnostics,
            "interpretation": self._build_model_interpretation(
                coefficients=coefficients,
                r_squared=r_squared,
                adjusted_r_squared=adjusted_r_squared,
                observations_count=len(targets),
                interpretation_context=interpretation_context,
                standard_errors_type=cluster_by,
            ),
        }

    def _estimate_covariance(
        self,
        *,
        x_matrix: np.ndarray,
        residuals_vector: np.ndarray,
        xtx_inv: np.ndarray,
        row_meta: list[dict[str, Any]],
        fallback_df: int,
        cluster_by: str,
    ) -> tuple[np.ndarray, int]:
        if cluster_by == "none":
            sigma2 = float(np.sum(residuals_vector**2)) / max(fallback_df, 1)
            return sigma2 * xtx_inv, fallback_df

        key_name = "object_name" if cluster_by == "object" else "year"
        groups: dict[str, list[int]] = {}
        for index, item in enumerate(row_meta):
            key = str(item.get(key_name, ""))
            groups.setdefault(key, []).append(index)
        if len(groups) <= 1:
            sigma2 = float(np.sum(residuals_vector**2)) / max(fallback_df, 1)
            return sigma2 * xtx_inv, fallback_df

        meat = np.zeros((x_matrix.shape[1], x_matrix.shape[1]))
        for indices in groups.values():
            xg = x_matrix[indices, :]
            ug = residuals_vector[indices]
            score = xg.T @ ug
            meat += np.outer(score, score)
        g = len(groups)
        n = x_matrix.shape[0]
        p = x_matrix.shape[1]
        correction = (g / max(g - 1, 1)) * ((n - 1) / max(n - p, 1))
        covariance = correction * (xtx_inv @ meat @ xtx_inv)
        return covariance, max(g - 1, 1)

    def _compute_vif(self, x_matrix: np.ndarray, feature_names: list[str]) -> list[dict[str, Any]]:
        if x_matrix.shape[1] <= 2:
            return []
        feature_matrix = x_matrix[:, 1:]
        vif_items: list[dict[str, Any]] = []
        for index, name in enumerate(feature_names):
            y = feature_matrix[:, index]
            others = np.delete(feature_matrix, index, axis=1)
            if others.shape[1] == 0:
                vif_items.append({"name": name, "vif": None})
                continue
            others_with_intercept = np.column_stack([np.ones(len(y)), others])
            beta = np.linalg.pinv(others_with_intercept.T @ others_with_intercept) @ others_with_intercept.T @ y
            predicted = others_with_intercept @ beta
            sst = float(np.sum((y - np.mean(y)) ** 2))
            sse = float(np.sum((y - predicted) ** 2))
            r_squared = 1.0 - (sse / sst) if sst > 0 else None
            vif = 1.0 / max(1.0 - r_squared, 1e-9) if r_squared is not None else None
            vif_items.append({"name": name, "vif": vif})
        return sorted(vif_items, key=lambda item: item["vif"] if item["vif"] is not None else -1, reverse=True)

    def _fit_regression(
        self,
        points: list[dict[str, Any]],
        regression_model: str,
        x_transform: str,
        y_transform: str,
    ) -> dict[str, Any]:
        model_points, design_matrix, targets, predictor_labels = self._prepare_regression_inputs(
            points,
            regression_model=regression_model,
            x_transform=x_transform,
            y_transform=y_transform,
        )
        equation_label = self._build_equation_label(
            response_label="y",
            coefficient_names=["Intercept", *predictor_labels],
            compress_year_effects=False,
        )
        return self._fit_ols_model(
            row_meta=model_points,
            design_matrix=design_matrix,
            targets=targets,
            coefficient_names=["Intercept", *predictor_labels],
            equation_label=equation_label,
            interpretation_context={
                "model": regression_model,
                "dropped_non_positive": len(points) - len(model_points),
                "response_label": "Y",
                "predictor_labels": predictor_labels,
                "include_year_fixed_effects": False,
                "transforms": {"x": x_transform, "y": y_transform},
                "analysis_type": "correlation",
            },
        )

    def _prepare_regression_inputs(
        self,
        points: list[dict[str, Any]],
        regression_model: str,
        x_transform: str,
        y_transform: str,
    ) -> tuple[list[dict[str, Any]], list[list[float]], list[float], list[str], Any]:
        prepared_points: list[dict[str, Any]] = []
        rows: list[list[float]] = []
        targets: list[float] = []
        predictor_labels: list[str] = []

        for item in points:
            original_x_value = float(item["x_value"])
            original_y_value = float(item["y_value"])
            x_value = self._apply_transform(original_x_value, x_transform)
            y_value = self._apply_transform(original_y_value, y_transform)
            if x_value is None or y_value is None:
                continue
            if regression_model == "linear":
                features = [x_value]
                target = y_value
                predictor_labels = ["x"]
            elif regression_model == "quadratic":
                features = [x_value, x_value**2]
                target = y_value
                predictor_labels = ["x", "x^2"]
            elif regression_model == "exponential":
                features = [x_value]
                if y_transform == "log":
                    target = y_value
                elif original_y_value <= 0:
                    continue
                else:
                    target = math.log(original_y_value)
                predictor_labels = ["x"]
            elif regression_model == "power":
                if x_transform == "log":
                    transformed_x = x_value
                elif original_x_value <= 0:
                    continue
                else:
                    transformed_x = math.log(original_x_value)
                if y_transform == "log":
                    target = y_value
                elif original_y_value <= 0:
                    continue
                else:
                    target = math.log(original_y_value)
                features = [transformed_x]
                predictor_labels = ["ln(x)"]
            else:
                features = [x_value]
                target = y_value
                predictor_labels = ["x"]

            prepared_points.append(item)
            rows.append([1.0, *features])
            targets.append(target)

        return prepared_points, rows, targets, predictor_labels

    def _build_equation_label(
        self,
        *,
        response_label: str,
        coefficient_names: list[str],
        compress_year_effects: bool,
        compress_object_effects: bool = False,
    ) -> str:
        parts = [response_label]
        non_fe_terms: list[str] = []
        year_effects = 0
        object_effects = 0
        for name in coefficient_names[1:]:
            if compress_year_effects and name.startswith("FE year "):
                year_effects += 1
                continue
            if compress_object_effects and name.startswith("FE object "):
                object_effects += 1
                continue
            non_fe_terms.append(name)
        parts.append("= Intercept")
        for name in non_fe_terms:
            parts.append(f"+ {name}")
        if year_effects:
            parts.append(f"+ {year_effects} year FE")
        if object_effects:
            parts.append(f"+ {object_effects} object FE")
        return " ".join(parts)

    def _build_model_interpretation(
        self,
        *,
        coefficients: list[dict[str, Any]],
        r_squared: float | None,
        adjusted_r_squared: float | None,
        observations_count: int,
        interpretation_context: dict[str, Any],
        standard_errors_type: str,
    ) -> dict[str, Any]:
        response_label = interpretation_context.get("response_label", "показатель")
        analysis_type = interpretation_context.get("analysis_type", "model")
        include_year_fixed_effects = bool(interpretation_context.get("include_year_fixed_effects"))
        include_object_fixed_effects = bool(interpretation_context.get("include_object_fixed_effects"))
        significant = [
            item
            for item in coefficients
            if item["name"] != "Intercept"
            and not str(item["name"]).startswith("FE year ")
            and not str(item["name"]).startswith("FE object ")
            and item["p_value"] is not None
            and item["p_value"] <= 0.05
        ]
        if adjusted_r_squared is None:
            fit_label = "не даёт устойчивого качества подгонки"
        elif adjusted_r_squared >= 0.65:
            fit_label = "хорошо объясняет различия в данных"
        elif adjusted_r_squared >= 0.35:
            fit_label = "объясняет заметную часть различий, но оставляет много необъяснённого"
        else:
            fit_label = "объясняет лишь небольшую часть различий"

        if significant:
            top = sorted(significant, key=lambda item: abs(item["t_stat"] or 0), reverse=True)[:3]
            driver_parts = []
            for item in top:
                direction = "рост" if (item["estimate"] or 0) > 0 else "снижение"
                driver_parts.append(f"{item['name']} показывает {direction} {response_label} (p={item['p_value']:.3f})")
            drivers_text = "; ".join(driver_parts)
        else:
            drivers_text = "статистически значимых драйверов на уровне 5% не найдено"

        headline = (
            f"Модель для {response_label} {fit_label}. "
            f"В выборке {observations_count} наблюдений."
        )
        bullets = [
            f"Adjusted R²: {adjusted_r_squared:.3f}" if adjusted_r_squared is not None else "Adjusted R² не определён.",
            f"R²: {r_squared:.3f}" if r_squared is not None else "R² не определён.",
            drivers_text[0].upper() + drivers_text[1:] if drivers_text else "Интерпретация драйверов недоступна.",
        ]
        if include_year_fixed_effects:
            bullets.append("В модели включены year fixed effects, поэтому сравнение очищено от общих межгодовых сдвигов.")
        if include_object_fixed_effects:
            bullets.append("В модели включены object fixed effects, поэтому сравнение очищено от постоянных различий между регионами.")
        lags = interpretation_context.get("lag_summary", [])
        if lags:
            bullets.append(f"Использованы лаги факторов: {', '.join(lags)}.")
        if standard_errors_type != "none":
            bullets.append(f"Стандартные ошибки кластеризованы по: {standard_errors_type}.")
        interactions = interpretation_context.get("interaction_summary", [])
        if interactions:
            bullets.append(f"В модели есть взаимодействия факторов: {', '.join(interactions[:3])}.")
        if analysis_type == "correlation":
            transforms = interpretation_context.get("transforms", {})
            bullets.append(
                f"Для корреляционного режима использованы transforms X={transforms.get('x', 'raw')} и Y={transforms.get('y', 'raw')}."
            )
        return {"headline": headline, "bullets": bullets}

    def _fetch_indicator_panel_rows(
        self,
        indicator: dict[str, Any],
        object_level: str,
        year_from: int,
        year_to: int,
    ) -> list[dict[str, Any]]:
        filters = ["indicator_code = ?", "object_level = ?", "year >= ?", "year <= ?"]
        params: list[Any] = [indicator["code"], object_level, year_from, year_to]
        if indicator.get("subsection"):
            filters.append("subsection = ?")
            params.append(indicator["subsection"])
        return self._query(
            f"""
            SELECT
              object_name,
              year,
              indicator_name,
              subsection,
              indicator_value,
              indicator_unit
            FROM observations
            WHERE {' AND '.join(filters)}
              AND indicator_value NOT IN ({MISSING_VALUE_SENTINELS[0]}, {MISSING_VALUE_SENTINELS[1]})
            ORDER BY year, object_name
            """,
            params,
        )

    def get_multi_regression_model(
        self,
        *,
        object_level: str,
        year_from: int,
        year_to: int,
        dependent_indicator: dict[str, Any],
        predictor_indicators: list[dict[str, Any]],
        include_year_fixed_effects: bool,
        include_object_fixed_effects: bool,
        cluster_by: str,
        include_pairwise_interactions: bool,
        event_study_indicator_code: str | None,
        event_study_max_lag_years: int,
    ) -> dict[str, Any]:
        def serialize_predictor(item: dict[str, Any]) -> dict[str, Any]:
            return {
                "code": item["code"],
                "subsection": item.get("subsection"),
                "transform": item.get("transform", "raw"),
                "lag_years": int(item.get("lag_years", 0) or 0),
            }

        predictors: list[dict[str, Any]] = []
        seen_predictors: set[tuple[str, str, int]] = set()
        for item in predictor_indicators:
            key = (
                str(item.get("code", "")),
                str(item.get("transform", "raw")),
                int(item.get("lag_years", 0) or 0),
            )
            if not key[0] or key[0] == dependent_indicator["code"] or key in seen_predictors:
                continue
            seen_predictors.add(key)
            predictors.append(
                {
                    **item,
                    "model_key": f"{key[0]}::{key[1]}::{key[2]}",
                }
            )
            if len(predictors) >= 5:
                break
        dependent_rows = self._fetch_indicator_panel_rows(
            dependent_indicator,
            object_level=object_level,
            year_from=year_from,
            year_to=year_to,
        )
        predictor_rows_by_code = {
            item["code"]: self._fetch_indicator_panel_rows(
                item,
                object_level=object_level,
                year_from=year_from,
                year_to=year_to,
            )
            for item in predictors
        }

        panel_map: dict[tuple[str, int], dict[str, Any]] = {}
        for row in dependent_rows:
            key = (str(row["object_name"]), int(row["year"]))
            transformed_value = self._apply_transform(float(row["indicator_value"]), dependent_indicator.get("transform", "raw"))
            if transformed_value is None:
                continue
            panel_map[key] = {
                "object_name": row["object_name"],
                "year": row["year"],
                "dependent_value": transformed_value,
                "dependent_raw_value": float(row["indicator_value"]),
                "dependent_indicator_name": row["indicator_name"],
                "dependent_unit": row["indicator_unit"],
                "predictors": {},
            }

        dropped_non_positive = 0
        for predictor in predictors:
            code = predictor["code"]
            model_key = predictor["model_key"]
            rows = predictor_rows_by_code.get(code, [])
            predictor_map: dict[tuple[str, int], float] = {}
            lag_years = int(predictor.get("lag_years", 0) or 0)
            for row in rows:
                transformed_value = self._apply_transform(float(row["indicator_value"]), predictor.get("transform", "raw"))
                if transformed_value is None:
                    dropped_non_positive += 1
                    continue
                predictor_map[(str(row["object_name"]), int(row["year"]) + lag_years)] = transformed_value
            for key in list(panel_map):
                value = predictor_map.get(key)
                if value is None:
                    panel_map.pop(key, None)
                    continue
                panel_map[key]["predictors"][model_key] = value

        panel_rows = sorted(panel_map.values(), key=lambda item: (item["year"], item["object_name"]))
        if not panel_rows or not predictors:
            return {
                "object_level": object_level,
                "year_from": year_from,
                "year_to": year_to,
                "dependent_indicator": dependent_indicator,
                "predictor_indicators": [serialize_predictor(item) for item in predictors],
                "include_year_fixed_effects": include_year_fixed_effects,
                "include_object_fixed_effects": include_object_fixed_effects,
                "cluster_by": cluster_by,
                "include_pairwise_interactions": include_pairwise_interactions,
                "observations_count": 0,
                "years": [],
                "regression": {
                    "model": "multi_linear",
                    "observations_count": 0,
                    "dropped_non_positive": dropped_non_positive,
                    "standard_errors_type": cluster_by,
                    "coefficients": [],
                    "r_squared": None,
                    "adjusted_r_squared": None,
                    "equation": None,
                    "residuals": [],
                    "interpretation": {
                        "headline": "Недостаточно данных для multi-factor модели.",
                        "bullets": ["Выберите зависимую переменную и хотя бы один фактор с достаточным покрытием."],
                    },
                },
                "predictor_summaries": [],
                "event_study": None,
            }

        years = sorted({int(item["year"]) for item in panel_rows})
        objects = sorted({str(item["object_name"]) for item in panel_rows})
        baseline_year = years[0]
        baseline_object = objects[0]
        coefficient_names = ["Intercept"]
        coefficient_names.extend(
            f"{item['code']} ({item.get('transform', 'raw')}, lag={int(item.get('lag_years', 0) or 0)})"
            for item in predictors
        )
        if include_year_fixed_effects:
            coefficient_names.extend(f"FE year {year}" for year in years[1:])
        if include_object_fixed_effects:
            coefficient_names.extend(f"FE object {object_name}" for object_name in objects[1:])

        design_matrix: list[list[float]] = []
        targets: list[float] = []
        interaction_names: list[str] = []
        if include_pairwise_interactions:
            for left_index, left in enumerate(predictors):
                for right in predictors[left_index + 1 :]:
                    interaction_names.append(f"{left['code']} x {right['code']}")
            coefficient_names.extend(interaction_names)
        for item in panel_rows:
            row = [1.0]
            for predictor in predictors:
                row.append(float(item["predictors"][predictor["model_key"]]))
            if include_year_fixed_effects:
                row.extend(1.0 if item["year"] == year else 0.0 for year in years[1:])
            if include_object_fixed_effects:
                row.extend(1.0 if item["object_name"] == object_name else 0.0 for object_name in objects[1:])
            if include_pairwise_interactions:
                for left_index, left in enumerate(predictors):
                    for right in predictors[left_index + 1 :]:
                        row.append(float(item["predictors"][left["model_key"]]) * float(item["predictors"][right["model_key"]]))
            design_matrix.append(row)
            targets.append(float(item["dependent_value"]))

        vif_feature_names: list[str] = []
        vif_column_indices: list[int] = [0]
        for index, name in enumerate(coefficient_names[1:], start=1):
            if name.startswith("FE year ") or name.startswith("FE object "):
                continue
            vif_feature_names.append(name)
            vif_column_indices.append(index)
        vif_items = self._compute_vif(
            np.asarray(design_matrix, dtype=float)[:, vif_column_indices],
            vif_feature_names,
        )

        regression = self._fit_ols_model(
            row_meta=panel_rows,
            design_matrix=design_matrix,
            targets=targets,
            coefficient_names=coefficient_names,
            equation_label=self._build_equation_label(
                response_label=f"{dependent_indicator['code']} ({dependent_indicator.get('transform', 'raw')})",
                coefficient_names=coefficient_names,
                compress_year_effects=True,
                compress_object_effects=True,
            ),
            interpretation_context={
                "model": "multi_linear",
                "dropped_non_positive": dropped_non_positive,
                "response_label": dependent_indicator["code"],
                "predictor_labels": [item["code"] for item in predictors],
                "include_year_fixed_effects": include_year_fixed_effects,
                "include_object_fixed_effects": include_object_fixed_effects,
                "lag_summary": [
                    f"{item['code']}@t-{int(item.get('lag_years', 0) or 0)}"
                    for item in predictors
                    if int(item.get("lag_years", 0) or 0) > 0
                ],
                "interaction_summary": interaction_names,
                "analysis_type": "multifactor",
            },
            cluster_by=cluster_by,
        )

        predictor_summaries = []
        for predictor in predictors:
            values = [float(item["predictors"][predictor["model_key"]]) for item in panel_rows]
            predictor_summaries.append(
                {
                    "code": predictor["code"],
                    "transform": predictor.get("transform", "raw"),
                    "lag_years": int(predictor.get("lag_years", 0) or 0),
                    "avg": float(sum(values) / len(values)) if values else None,
                    "min": float(min(values)) if values else None,
                    "max": float(max(values)) if values else None,
                }
            )

        return {
            "object_level": object_level,
            "year_from": year_from,
            "year_to": year_to,
            "dependent_indicator": dependent_indicator,
            "predictor_indicators": [serialize_predictor(item) for item in predictors],
            "include_year_fixed_effects": include_year_fixed_effects,
            "include_object_fixed_effects": include_object_fixed_effects,
            "cluster_by": cluster_by,
            "include_pairwise_interactions": include_pairwise_interactions,
            "observations_count": len(panel_rows),
            "years": years,
            "baseline_year": baseline_year,
            "baseline_object": baseline_object,
            "regression": regression,
            "predictor_summaries": predictor_summaries,
            "collinearity": {
                "vif_items": vif_items,
                "high_vif_count": sum(1 for item in vif_items if item["vif"] is not None and item["vif"] >= 10),
                "warning_level": (
                    "high"
                    if any(item["vif"] is not None and item["vif"] >= 10 for item in vif_items)
                    else "moderate"
                    if any(item["vif"] is not None and item["vif"] >= 5 for item in vif_items)
                    else "low"
                ),
            },
            "event_study": self._build_event_study_profile(
                panel_rows=panel_rows,
                predictor_indicators=predictors,
                event_study_indicator_code=event_study_indicator_code,
                max_lag_years=event_study_max_lag_years,
            ),
        }

    def _build_event_study_profile(
        self,
        *,
        panel_rows: list[dict[str, Any]],
        predictor_indicators: list[dict[str, Any]],
        event_study_indicator_code: str | None,
        max_lag_years: int,
    ) -> dict[str, Any] | None:
        if not panel_rows:
            return None
        target_code = event_study_indicator_code or predictor_indicators[0]["code"]
        target_predictor = next((item for item in predictor_indicators if item["code"] == target_code), None)
        if target_predictor is None:
            return None
        model_key = target_predictor["model_key"]
        panel_by_object: dict[str, dict[int, float]] = {}
        for row in panel_rows:
            panel_by_object.setdefault(str(row["object_name"]), {})[int(row["year"])] = float(row["predictors"][model_key])
        lag_points = []
        for lag in range(0, max(0, max_lag_years) + 1):
            xs: list[float] = []
            ys: list[float] = []
            for row in panel_rows:
                object_name = str(row["object_name"])
                year = int(row["year"])
                lagged_value = panel_by_object.get(object_name, {}).get(year - lag)
                if lagged_value is None:
                    continue
                xs.append(lagged_value)
                ys.append(float(row["dependent_value"]))
            if len(xs) < 3:
                lag_points.append({"lag_years": lag, "observations_count": len(xs), "correlation": None, "slope": None})
                continue
            x_avg = sum(xs) / len(xs)
            y_avg = sum(ys) / len(ys)
            sxx = sum((value - x_avg) ** 2 for value in xs)
            sxy = sum((x - x_avg) * (y - y_avg) for x, y in zip(xs, ys, strict=False))
            syy = sum((value - y_avg) ** 2 for value in ys)
            slope = sxy / sxx if sxx > 0 else None
            correlation = sxy / math.sqrt(sxx * syy) if sxx > 0 and syy > 0 else None
            lag_points.append(
                {
                    "lag_years": lag,
                    "observations_count": len(xs),
                    "correlation": correlation,
                    "slope": slope,
                }
            )
        strongest_lag = max(
            lag_points,
            key=lambda item: abs(item["correlation"]) if item["correlation"] is not None else -1,
            default=None,
        )
        return {
            "indicator_code": target_code,
            "points": lag_points,
            "strongest_lag": strongest_lag,
        }

    def build_report_brief(
        self,
        title: str,
        cards: list[dict[str, Any]],
        saved_views_count: int,
        explorer_normalization: str | None,
        dataset_rows: int | None,
    ) -> dict[str, Any]:
        kind_counts: dict[str, int] = {}
        for card in cards:
            kind = str(card.get("kind") or "other")
            kind_counts[kind] = kind_counts.get(kind, 0) + 1

        summary_lines = [
            f"Cards: {len(cards)}",
            f"Saved views: {saved_views_count}",
        ]
        if dataset_rows is not None:
            summary_lines.append(f"Dataset rows: {dataset_rows}")
        if explorer_normalization:
            summary_lines.append(f"Explorer normalization: {explorer_normalization}")

        markdown_parts = [f"# {title}", "", "## Executive Summary", ""]
        markdown_parts.extend(f"- {line}" for line in summary_lines)
        markdown_parts.extend(["", "## Story Mix", ""])
        markdown_parts.extend(f"- {kind}: {count}" for kind, count in sorted(kind_counts.items()))
        markdown_parts.extend(["", "## Sections", ""])

        for index, card in enumerate(cards, start=1):
            markdown_parts.extend(
                [
                    f"### {index}. {card.get('title', 'Untitled')}",
                    "",
                    str(card.get("subtitle", "")),
                    "",
                    f"- Key point: {card.get('primary', '')}",
                    f"- Support point: {card.get('secondary', '')}",
                    *[f"- {note}" for note in card.get("notes", [])],
                    "",
                ]
            )

        markdown = "\n".join(markdown_parts)
        return {
            "title": title,
            "cards_count": len(cards),
            "kind_counts": kind_counts,
            "markdown": markdown,
            "summary_lines": summary_lines,
        }

    def build_report_pdf(
        self,
        title: str,
        cards: list[dict[str, Any]],
        saved_views_count: int,
        explorer_normalization: str | None,
        dataset_rows: int | None,
    ) -> bytes:
        brief = self.build_report_brief(
            title=title,
            cards=cards,
            saved_views_count=saved_views_count,
            explorer_normalization=explorer_normalization,
            dataset_rows=dataset_rows,
        )
        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4

        pdf.setFillColor(colors.HexColor("#0f172a"))
        pdf.rect(0, 0, width, height, fill=1, stroke=0)
        pdf.setFillColor(colors.white)
        pdf.setFont("Helvetica-Bold", 24)
        pdf.drawString(48, height - 72, title)
        pdf.setFont("Helvetica", 12)
        pdf.drawString(48, height - 98, "Nienna server-rendered analytical memo")
        pdf.drawString(48, height - 118, f"Cards: {brief['cards_count']} · Saved views: {saved_views_count}")
        pdf.showPage()

        y = height - 48
        pdf.setFillColor(colors.black)
        pdf.setFont("Helvetica-Bold", 18)
        pdf.drawString(40, y, "Executive Summary")
        y -= 28
        pdf.setFont("Helvetica", 10)
        for line in brief["summary_lines"]:
            pdf.drawString(44, y, f"- {line}")
            y -= 16

        y -= 10
        pdf.setFont("Helvetica-Bold", 16)
        pdf.drawString(40, y, "Story Mix")
        y -= 24
        pdf.setFont("Helvetica", 10)
        for kind, count in sorted(brief["kind_counts"].items()):
            pdf.drawString(44, y, f"{kind}: {count}")
            y -= 16

        for index, card in enumerate(cards, start=1):
            if y < 120:
                pdf.showPage()
                y = height - 48
            pdf.setFont("Helvetica-Bold", 13)
            pdf.drawString(40, y, f"{index}. {card.get('title', 'Untitled')}")
            y -= 16
            pdf.setFont("Helvetica", 10)
            lines = [
                str(card.get("subtitle", "")),
                f"Key point: {card.get('primary', '')}",
                f"Support point: {card.get('secondary', '')}",
                *[f"- {note}" for note in card.get("notes", [])],
            ]
            for line in lines:
                for wrapped in self._wrap_pdf_line(line, 88):
                    if y < 80:
                        pdf.showPage()
                        y = height - 48
                    pdf.drawString(44, y, wrapped)
                    y -= 14
            y -= 10

        pdf.save()
        return buffer.getvalue()

    def _wrap_pdf_line(self, value: str, width: int) -> list[str]:
        words = value.split()
        if not words:
            return [""]
        lines: list[str] = []
        current = words[0]
        for word in words[1:]:
            candidate = f"{current} {word}"
            if len(candidate) <= width:
                current = candidate
            else:
                lines.append(current)
                current = word
        lines.append(current)
        return lines


@lru_cache(maxsize=1)
def get_dataset_service() -> DatasetService:
    return DatasetService(DATASET_PATH)
