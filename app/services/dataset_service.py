from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATASET_PATH = PROJECT_ROOT / "data" / "normalized" / "rosstat" / "data_regions_collection_102_v20260313.parquet"


class DatasetService:
    def __init__(self, dataset_path: Path) -> None:
        self.dataset_path = dataset_path

    def get_dataset_summary(self) -> dict[str, Any]:
        parquet_file = pq.ParquetFile(self.dataset_path)
        table = parquet_file.read()
        sample_table = table.slice(0, 5)

        region_candidates = [
            name
            for name in table.column_names
            if "region" in name.lower() or "регион" in name.lower()
        ]

        preview_columns = []
        for name in table.column_names[:20]:
            column = table.column(name)
            preview_columns.append(
                {
                    "name": name,
                    "type": str(column.type),
                    "null_count": int(column.null_count),
                }
            )

        return {
            "dataset": self.dataset_path.name,
            "path": str(self.dataset_path.relative_to(PROJECT_ROOT)),
            "file_size_bytes": self.dataset_path.stat().st_size,
            "rows": table.num_rows,
            "columns_count": table.num_columns,
            "columns": table.column_names,
            "preview_columns": preview_columns,
            "region_column_candidates": region_candidates,
            "sample_records": sample_table.to_pylist(),
        }


@lru_cache(maxsize=1)
def get_dataset_service() -> DatasetService:
    return DatasetService(DATASET_PATH)
