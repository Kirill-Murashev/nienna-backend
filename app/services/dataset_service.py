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
        regression = self._fit_regression(points, regression_model)

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

    def _fit_regression(self, points: list[dict[str, Any]], regression_model: str) -> dict[str, Any]:
        model_points, design_matrix, targets, predictor_labels, equation_template = self._prepare_regression_inputs(
            points, regression_model
        )
        if len(model_points) < len(predictor_labels) + 2:
            return {
                "model": regression_model,
                "observations_count": len(model_points),
                "dropped_non_positive": len(points) - len(model_points),
                "coefficients": [],
                "r_squared": None,
                "adjusted_r_squared": None,
                "equation": None,
                "residuals": [],
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
        sst = float(np.sum((y_vector - np.mean(y_vector)) ** 2))
        r_squared = 1.0 - (sse / sst) if sst > 0 else None
        adjusted_r_squared = (
            1.0 - (1.0 - r_squared) * (n - 1) / df if r_squared is not None and df > 0 else None
        )
        sigma2 = sse / df if df > 0 else 0.0
        covariance = sigma2 * xtx_inv
        standard_errors = np.sqrt(np.diag(covariance))
        critical_t = float(student_t.ppf(0.975, df)) if df > 0 else 0.0

        coefficient_names = ["Intercept", *predictor_labels]
        coefficients: list[dict[str, Any]] = []
        for index, name in enumerate(coefficient_names):
            estimate = float(beta[index])
            se = float(standard_errors[index]) if index < len(standard_errors) else 0.0
            t_stat = estimate / se if se > 0 else None
            p_value = float(2 * (1 - student_t.cdf(abs(t_stat), df))) if t_stat is not None and df > 0 else None
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
            model_points, y_vector.tolist(), predicted.tolist(), residuals_vector.tolist(), strict=False
        ):
            residuals.append(
                {
                    "object_name": item["object_name"],
                    "predicted_y": predicted_value,
                    "actual_y": actual,
                    "residual": residual,
                }
            )

        return {
            "model": regression_model,
            "observations_count": len(model_points),
            "dropped_non_positive": len(points) - len(model_points),
            "coefficients": coefficients,
            "r_squared": r_squared,
            "adjusted_r_squared": adjusted_r_squared,
            "equation": equation_template(coefficients),
            "residuals": sorted(residuals, key=lambda item: abs(item["residual"]), reverse=True)[:12],
        }

    def _prepare_regression_inputs(
        self, points: list[dict[str, Any]], regression_model: str
    ) -> tuple[list[dict[str, Any]], list[list[float]], list[float], list[str], Any]:
        prepared_points: list[dict[str, Any]] = []
        rows: list[list[float]] = []
        targets: list[float] = []

        for item in points:
            x_value = float(item["x_value"])
            y_value = float(item["y_value"])
            if regression_model == "linear":
                features = [x_value]
                target = y_value
                predictor_labels = ["x"]
                equation_template = lambda coeffs: f"y = {coeffs[0]['estimate']:.4f} + {coeffs[1]['estimate']:.4f} * x"
            elif regression_model == "quadratic":
                features = [x_value, x_value**2]
                target = y_value
                predictor_labels = ["x", "x^2"]
                equation_template = (
                    lambda coeffs: f"y = {coeffs[0]['estimate']:.4f} + {coeffs[1]['estimate']:.4f} * x + {coeffs[2]['estimate']:.4f} * x^2"
                )
            elif regression_model == "exponential":
                if y_value <= 0:
                    continue
                features = [x_value]
                target = math.log(y_value)
                predictor_labels = ["x"]
                equation_template = (
                    lambda coeffs: f"ln(y) = {coeffs[0]['estimate']:.4f} + {coeffs[1]['estimate']:.4f} * x"
                )
            elif regression_model == "power":
                if x_value <= 0 or y_value <= 0:
                    continue
                features = [math.log(x_value)]
                target = math.log(y_value)
                predictor_labels = ["ln(x)"]
                equation_template = (
                    lambda coeffs: f"ln(y) = {coeffs[0]['estimate']:.4f} + {coeffs[1]['estimate']:.4f} * ln(x)"
                )
            else:
                features = [x_value]
                target = y_value
                predictor_labels = ["x"]
                equation_template = lambda coeffs: f"y = {coeffs[0]['estimate']:.4f} + {coeffs[1]['estimate']:.4f} * x"

            prepared_points.append(item)
            rows.append([1.0, *features])
            targets.append(target)

        return prepared_points, rows, targets, predictor_labels, equation_template

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
