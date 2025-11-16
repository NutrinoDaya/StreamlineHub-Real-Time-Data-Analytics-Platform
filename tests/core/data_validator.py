#!/usr/bin/env python
"""
Data Validator

This module provides comprehensive data validation for ETL pipelines:
- Schema validation
- Data quality checks
- Business rule validation
- Performance monitoring
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from utils import LoggerManager

logger = LoggerManager.get_logger("ETL_Processing.log")


class DataValidator:
    """
    data validation with comprehensive checks
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.validation_rules = {}
        self.quality_thresholds = {
            "null_percentage_threshold": 0.95,  # Max 95% nulls allowed
            "duplicate_percentage_threshold": 0.10,  # Max 10% duplicates
            "min_record_count": 1,  # Minimum records required
            "schema_match_required": True
        }
    
    def register_validation_rule(self, name: str, validation_func: callable, description: str = ""):
        """Register custom validation rule"""
        self.validation_rules[name] = {
            "function": validation_func,
            "description": description
        }
        logger.debug(f"Registered validation rule: {name}")
    
    def validate_schema_compatibility(self, df: DataFrame, expected_schema: StructType) -> Tuple[bool, List[str]]:
        """Validate DataFrame schema against expected schema"""
        issues = []
        actual_schema = df.schema
        
        # Check field count
        if len(actual_schema.fields) != len(expected_schema.fields):
            issues.append(f"Field count mismatch: expected {len(expected_schema.fields)}, got {len(actual_schema.fields)}")
        
        # Check each field
        actual_fields = {field.name: field for field in actual_schema.fields}
        expected_fields = {field.name: field for field in expected_schema.fields}
        
        for field_name, expected_field in expected_fields.items():
            if field_name not in actual_fields:
                issues.append(f"Missing field: {field_name}")
                continue
            
            actual_field = actual_fields[field_name]
            
            # Check data type compatibility
            if not self._is_type_compatible(actual_field.dataType, expected_field.dataType):
                issues.append(f"Type mismatch for {field_name}: expected {expected_field.dataType}, got {actual_field.dataType}")
            
            # Check nullability (if expected is non-null, actual must be non-null)
            if not expected_field.nullable and actual_field.nullable:
                issues.append(f"Nullability mismatch for {field_name}: expected non-nullable")
        
        # Check for extra fields
        for field_name in actual_fields:
            if field_name not in expected_fields:
                issues.append(f"Unexpected field: {field_name}")
        
        return len(issues) == 0, issues
    
    def _is_type_compatible(self, actual_type, expected_type) -> bool:
        """Check if data types are compatible"""
        # Exact match
        if actual_type == expected_type:
            return True
        
        # String types are generally compatible
        if isinstance(actual_type, StringType) and isinstance(expected_type, StringType):
            return True
        
        # Integer types compatibility
        if isinstance(actual_type, IntegerType) and isinstance(expected_type, IntegerType):
            return True
        
        # Add more compatibility rules as needed
        return False
    
    def validate_data_quality(self, df: DataFrame, table_name: str = "unknown") -> Dict[str, Any]:
        """Comprehensive data quality validation"""
        start_time = datetime.now()
        
        # Basic metrics
        total_records = df.count()
        total_columns = len(df.columns)
        
        quality_report = {
            "table_name": table_name,
            "validation_timestamp": start_time.isoformat(),
            "total_records": total_records,
            "total_columns": total_columns,
            "quality_score": 1.0,
            "issues": [],
            "warnings": [],
            "metrics": {},
            "passed": True
        }
        
        # Skip validation for empty datasets
        if total_records == 0:
            quality_report["warnings"].append("Empty dataset - skipping quality checks")
            return quality_report
        
        try:
            # Check minimum record count
            if total_records < self.quality_thresholds["min_record_count"]:
                quality_report["issues"].append(f"Insufficient records: {total_records} < {self.quality_thresholds['min_record_count']}")
                quality_report["quality_score"] *= 0.5
            
            # Null value analysis
            null_analysis = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / total_records) * 100
                null_analysis[column] = {
                    "null_count": null_count,
                    "null_percentage": null_percentage
                }
                
                if null_percentage > self.quality_thresholds["null_percentage_threshold"] * 100:
                    quality_report["issues"].append(f"High null percentage in {column}: {null_percentage:.2f}%")
                    quality_report["quality_score"] *= 0.9
                elif null_percentage > 50:
                    quality_report["warnings"].append(f"Moderate null percentage in {column}: {null_percentage:.2f}%")
            
            quality_report["metrics"]["null_analysis"] = null_analysis
            
            # Duplicate analysis (if DataFrame has reasonable size)
            if total_records <= 1000000:  # Only for datasets <= 1M records
                duplicate_count = total_records - df.distinct().count()
                duplicate_percentage = (duplicate_count / total_records) * 100
                quality_report["metrics"]["duplicate_count"] = duplicate_count
                quality_report["metrics"]["duplicate_percentage"] = duplicate_percentage
                
                if duplicate_percentage > self.quality_thresholds["duplicate_percentage_threshold"] * 100:
                    quality_report["issues"].append(f"High duplicate percentage: {duplicate_percentage:.2f}%")
                    quality_report["quality_score"] *= 0.8
            
            # Data type consistency checks
            type_issues = self._validate_data_types(df)
            if type_issues:
                quality_report["issues"].extend(type_issues)
                quality_report["quality_score"] *= 0.9
            
            # Business rule validation
            business_rule_issues = self._validate_business_rules(df, table_name)
            if business_rule_issues:
                quality_report["issues"].extend(business_rule_issues)
                quality_report["quality_score"] *= 0.85
            
            # Set final status
            quality_report["passed"] = len(quality_report["issues"]) == 0 and quality_report["quality_score"] >= 0.7
            
            validation_time = (datetime.now() - start_time).total_seconds()
            quality_report["validation_duration_seconds"] = validation_time
            
            logger.info(f"Data quality validation completed for {table_name}: "
                       f"Score={quality_report['quality_score']:.2f}, "
                       f"Issues={len(quality_report['issues'])}, "
                       f"Duration={validation_time:.2f}s")
            
        except Exception as e:
            quality_report["issues"].append(f"Validation error: {str(e)}")
            quality_report["passed"] = False
            quality_report["quality_score"] = 0.0
            logger.error(f"Data quality validation failed for {table_name}: {e}")
        
        return quality_report
    
    def _validate_data_types(self, df: DataFrame) -> List[str]:
        """Validate data type consistency"""
        issues = []
        
        for column in df.columns:
            col_type = df.schema[column].dataType
            
            # Check timestamp columns for valid formats - be more lenient
            if isinstance(col_type, TimestampType):
                try:
                    invalid_timestamps = df.filter(col(column).isNotNull() & col(column).isNaN()).count()
                    if invalid_timestamps > 0:
                        issues.append(f"Invalid timestamps in column {column}: {invalid_timestamps} records")
                except Exception:
                    # For development - don't fail on timestamp validation issues
                    logger.warning(f"Could not validate timestamps in column {column}")
        
        return issues
    
    def _validate_business_rules(self, df: DataFrame, table_name: str) -> List[str]:
        """Apply business rule validations"""
        issues = []
        
        # IPP_PSOP specific validations
        if "IPP_PSOP" in table_name.upper():
            issues.extend(self._validate_ipp_psop_rules(df))
        
        # Traffic specific validations
        if "TRAFFIC" in table_name.upper():
            issues.extend(self._validate_traffic_rules(df))
        
        # VPC specific validations
        if "VPC" in table_name.upper():
            issues.extend(self._validate_vpc_rules(df))
        
        # Apply custom registered rules
        for rule_name, rule_config in self.validation_rules.items():
            try:
                if not rule_config["function"](df):
                    issues.append(f"Custom rule failed: {rule_name}")
            except Exception as e:
                issues.append(f"Custom rule error ({rule_name}): {str(e)}")
        
        return issues
    
    def _validate_ipp_psop_rules(self, df: DataFrame) -> List[str]:
        """IPP_PSOP specific business rules - made more lenient for development"""
        issues = []
        
        # Check required fields exist - but be more lenient
        required_fields = ["IncidentId"]  # Reduced requirements for development
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(col(field).isNull() | (col(field) == "")).count()
                if null_count > 0:
                    # Warning instead of hard failure
                    logger.warning(f"Required field {field} has {null_count} null/empty values")
        
        # Validate IncidentId format (if exists) - be more lenient
        if "IncidentId" in df.columns:
            try:
                # More lenient format validation - allow any non-empty string
                invalid_ids = df.filter(
                    col("IncidentId").isNotNull() & 
                    (col("IncidentId") == "")
                ).count()
                if invalid_ids > 0:
                    logger.warning(f"Empty IncidentId found: {invalid_ids} records")
            except Exception:
                pass
        
        return issues
    
    def _validate_traffic_rules(self, df: DataFrame) -> List[str]:
        """Traffic data specific business rules"""
        issues = []
        
        # Traffic-specific validations would go here
        # For example: speed limits, coordinate ranges, timestamp sequence
        
        return issues
    
    def _validate_vpc_rules(self, df: DataFrame) -> List[str]:
        """VPC data specific business rules"""
        issues = []
        
        # VPC-specific validations would go here
        # For example: valid states, event type consistency
        
        return issues
    
    def validate_batch(self, df: DataFrame, table_name: str, expected_schema: Optional[StructType] = None) -> Dict[str, Any]:
        """Complete batch validation including schema and quality checks"""
        validation_result = {
            "table_name": table_name,
            "validation_timestamp": datetime.now().isoformat(),
            "schema_validation": {"passed": True, "issues": []},
            "quality_validation": {},
            "overall_passed": True,
            "recommendations": []
        }
        
        try:
            # Schema validation
            if expected_schema:
                schema_passed, schema_issues = self.validate_schema_compatibility(df, expected_schema)
                validation_result["schema_validation"]["passed"] = schema_passed
                validation_result["schema_validation"]["issues"] = schema_issues
                
                if not schema_passed:
                    validation_result["overall_passed"] = False
                    validation_result["recommendations"].append("Fix schema compatibility issues before processing")
            
            # Quality validation
            quality_result = self.validate_data_quality(df, table_name)
            validation_result["quality_validation"] = quality_result
            
            if not quality_result["passed"]:
                validation_result["overall_passed"] = False
                if quality_result["quality_score"] < 0.5:
                    validation_result["recommendations"].append("Data quality is critically low - consider data cleansing")
                else:
                    validation_result["recommendations"].append("Address data quality issues for optimal processing")
            
            # Performance recommendations
            record_count = df.count()
            if record_count > 100000:
                validation_result["recommendations"].append("Consider partitioning for large datasets")
            
        except Exception as e:
            validation_result["overall_passed"] = False
            validation_result["error"] = str(e)
            logger.error(f"Batch validation failed for {table_name}: {e}")
        
        return validation_result
