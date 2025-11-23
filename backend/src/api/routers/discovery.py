from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import os
from pathlib import Path
from deltalake import DeltaTable
import logging

router = APIRouter(tags=["Data Discovery"])
logger = logging.getLogger(__name__)

BRONZE_PATH = os.getenv("BRONZE_DATA_PATH", "/app/data/bronze")

def get_table_stats(table_path: str) -> Dict[str, Any]:
    try:
        dt = DeltaTable(table_path)
        # Get files to calculate size
        files = dt.files()
        total_size = 0
        # This is a rough estimate, for exact size we might need to inspect file metadata if available in delta-rs
        # delta-rs doesn't expose file size directly in files() list easily without iterating add actions
        # But we can use the file system since we have the path
        
        # Calculate size from file system for the table directory
        root_directory = Path(table_path)
        total_size = sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file())
        
        # Get record count - delta-rs might not have a direct count without scanning, 
        # but we can try to get it from metadata or scan. 
        # For fast response, we might rely on add actions metadata if available.
        # dt.to_pyarrow_dataset().count_rows() is fast enough usually.
        record_count = dt.to_pyarrow_dataset().count_rows()

        return {
            "record_count": record_count,
            "size_bytes": total_size
        }
    except Exception as e:
        logger.error(f"Error getting stats for {table_path}: {e}")
        return {"record_count": 0, "size_bytes": 0}

@router.get("/tables", response_model=List[Dict[str, Any]])
async def list_tables():
    """List all bronze tables with summary stats."""
    tables = []
    try:
        base_path = Path(BRONZE_PATH)
        if not base_path.exists():
            return []

        for entry in base_path.iterdir():
            if entry.is_dir() and (entry / "_delta_log").exists():
                stats = get_table_stats(str(entry))
                tables.append({
                    "name": entry.name,
                    "path": str(entry),
                    "record_count": stats["record_count"],
                    "size_bytes": stats["size_bytes"]
                })
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    return tables

@router.get("/tables/{table_name}", response_model=Dict[str, Any])
async def get_table_details(table_name: str):
    """Get schema, stats, and preview data for a specific table."""
    table_path = os.path.join(BRONZE_PATH, table_name)
    if not os.path.exists(table_path):
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        dt = DeltaTable(table_path)
        
        # Get Stats
        stats = get_table_stats(table_path)
        
        # Get Preview Data (10 records) and extract schema from DataFrame
        df = dt.to_pandas()
        df_preview = df.head(10)  # Get first 10 records
        
        # Handle NaN and infinity values that cause JSON serialization issues
        import pandas as pd
        import numpy as np
        import json
        from decimal import Decimal
        
        # Simple but robust approach: convert DataFrame values to JSON-safe types
        preview_data = []
        for _, row in df_preview.iterrows():
            row_dict = {}
            for col, value in row.items():
                # Handle different data types safely
                if pd.isna(value):
                    row_dict[col] = None
                elif isinstance(value, (np.integer, int)):
                    row_dict[col] = int(value)
                elif isinstance(value, (np.floating, float)):
                    if np.isfinite(value):
                        # Check if the float value is within JSON range
                        if abs(value) < 1.7976931348623157e+308:  # Max float64 that's JSON safe
                            row_dict[col] = float(value)
                        else:
                            row_dict[col] = str(value)  # Convert large numbers to string
                    else:
                        row_dict[col] = None  # NaN or infinity
                elif isinstance(value, (str, bool)):
                    row_dict[col] = value
                elif hasattr(value, 'isoformat'):  # datetime objects
                    row_dict[col] = value.isoformat()
                else:
                    # For any other type, convert to string as fallback
                    row_dict[col] = str(value)
            preview_data.append(row_dict)
        
        # Get Schema from DataFrame dtypes
        schema_fields = []
        for col_name, dtype in df.dtypes.items():
            schema_fields.append({
                "name": col_name,
                "type": str(dtype),
                "nullable": True  # Pandas doesn't easily expose nullable info
            })
        schema_json = {
            "type": "struct", 
            "fields": schema_fields
        }
        
        return {
            "name": table_name,
            "schema": schema_json,
            "record_count": stats["record_count"],
            "size_bytes": stats["size_bytes"],
            "preview": preview_data
        }
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
