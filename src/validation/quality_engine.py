from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)
from pyspark.sql.window import Window


class DataQualityEngine:
    def __init__(self, run_id, pipeline_name, task_name):
        self.run_id = run_id
        self.pipeline_name = pipeline_name
        self.task_name = task_name

    def validate(
        self,
        df,
        expected_schema=None,
        required_columns=None,
        unique_columns=None,
        min_rows=None,
        reference_data=None,
        range_rules=None,
        custom_rules=None,
    ):
        expected_schema = expected_schema or {}
        required_columns = required_columns or []
        unique_columns = unique_columns or []
        reference_data = reference_data or {}
        range_rules = range_rules or {}
        custom_rules = custom_rules or []

        records_checked = df.count()

        # Generate internal column names without overwriting source columns.
        used_columns = set(df.columns)

        def new_internal_column(base_name):
            name = base_name
            suffix = 1

            while name in used_columns:
                name = f"{base_name}_{suffix}"
                suffix += 1

            used_columns.add(name)
            return name

        rule_conditions = {
            "schema": [],
            "null": [],
            "duplicate": [],
            "referential_integrity": [],
            "range": [],
        }

        schema_errors = []
        internal_columns = []

        def add_rule(rule_name, condition, message):
            rule_conditions[rule_name].append((condition, message))

        # 1. Schema and Data Types Validation
        for column, expected_type in expected_schema.items():
            if column not in df.columns:
                message = f"Missing column: {column}"
                schema_errors.append(message)

                add_rule("schema", F.lit(True), message)
                continue

            actual_type = df.schema[column].dataType.simpleString()

            if hasattr(expected_type, "simpleString"):
                expected_type = expected_type.simpleString()

            if actual_type.lower() != str(expected_type).lower():
                message = (
                    f"Invalid type for {column}: expected "
                    f"{expected_type}, got {actual_type}"
                )

                schema_errors.append(message)
                add_rule("schema", F.lit(True), message)

        # Custom row-level rules (e.g. from safe_cast unparseable checks)
        for custom_rule in custom_rules:
            if isinstance(custom_rule, tuple) and len(custom_rule) == 2:
                cond, msg = custom_rule
                add_rule("schema", cond, msg)
                schema_errors.append(msg)

        # 2. NULL & Empty String Validation
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(
                    f"Required column does not exist: {column}"
                )

            # NULL check
            add_rule(
                "null",
                F.col(column).isNull(),
                f"{column} cannot be NULL",
            )

            # Empty string check for string columns using trim()
            col_type = df.schema[column].dataType.simpleString().lower()
            if col_type in ("string", "text"):
                add_rule(
                    "null",
                    F.col(column).isNotNull() & (F.trim(F.col(column)) == ""),
                    f"{column} cannot be empty",
                )

        # 3. Duplicate Detection
        if unique_columns:
            missing = [
                column for column in unique_columns
                if column not in df.columns
            ]

            if missing:
                raise ValueError(
                    f"Unique columns do not exist: {missing}"
                )

            duplicate_column = new_internal_column(
                "_dq_duplicate_flag"
            )

            duplicate_window = Window.partitionBy(*unique_columns)

            df = df.withColumn(
                duplicate_column,
                F.count("*").over(duplicate_window) > 1,
            )

            internal_columns.append(duplicate_column)

            add_rule(
                "duplicate",
                F.col(duplicate_column),
                "Duplicate record",
            )

        # 4. Referential Integrity
        for index, (key, ref_spec) in enumerate(
            reference_data.items()
        ):
            if isinstance(ref_spec, tuple) and len(ref_spec) == 2:
                reference_df, ref_col = ref_spec
                fk_col = key
            elif isinstance(ref_spec, dict) and "df" in ref_spec:
                reference_df = ref_spec["df"]
                ref_col = ref_spec.get("col", key)
                fk_col = key
            else:
                reference_df = ref_spec
                ref_col = key
                fk_col = key

            if fk_col not in df.columns:
                raise ValueError(
                    f"Foreign key column does not exist: {fk_col}"
                )

            if ref_col not in reference_df.columns:
                raise ValueError(
                    f"Reference column does not exist: {ref_col}"
                )

            reference_column = new_internal_column(
                f"_dq_reference_{index}"
            )

            reference_values = (
                reference_df
                .select(F.col(ref_col).alias("_dq_ref_value"))
                .filter(F.col(ref_col).isNotNull())
                .dropDuplicates()
                .withColumn("_dq_found", F.lit(True))
                .alias("reference")
            )

            source = df.alias("source")

            df = (
                source.join(
                    reference_values,
                    F.col(f"source.`{fk_col}`")
                    == F.col("reference._dq_ref_value"),
                    "left",
                )
                .select(
                    "source.*",
                    F.col("reference._dq_found").alias(
                        reference_column
                    ),
                )
            )

            msg = (
                f"{fk_col} has no matching reference"
                if fk_col == ref_col
                else f"{fk_col} has no matching reference in {ref_col}"
            )

            add_rule(
                "referential_integrity",
                F.col(fk_col).isNotNull()
                & F.col(reference_column).isNull(),
                msg,
            )

            internal_columns.append(reference_column)

        # 5. Range & Date Validation
        for column, limits in range_rules.items():
            if column not in df.columns:
                raise ValueError(
                    f"Range column does not exist: {column}"
                )

            inclusive = True
            if isinstance(limits, (list, tuple)):
                minimum = limits[0]
                maximum = limits[1] if len(limits) > 1 else None
                if len(limits) > 2:
                    inclusive = bool(limits[2])
            elif isinstance(limits, dict):
                minimum = limits.get("min")
                maximum = limits.get("max")
                inclusive = limits.get("inclusive", True)
            else:
                raise ValueError(
                    f"Invalid range rule format for column '{column}': {limits}"
                )

            col_type = df.schema[column].dataType.simpleString().lower()
            is_date_col = (
                col_type in ("date", "timestamp")
                or (isinstance(maximum, str) and maximum.lower() in ("current_date", "today", "now"))
                or (isinstance(minimum, str) and minimum.lower() in ("current_date", "today", "now"))
            )

            if is_date_col:
                date_expr = (
                    F.to_date(F.col(column))
                    if col_type not in ("date", "timestamp")
                    else F.col(column)
                )

                if maximum is not None:
                    if str(maximum).lower() in ("current_date", "today", "now"):
                        max_date_expr = F.current_date()
                        max_label = "current_date"
                    else:
                        max_date_expr = F.to_date(F.lit(maximum))
                        max_label = str(maximum)
                else:
                    max_date_expr = None

                if minimum is not None:
                    if str(minimum).lower() in ("current_date", "today", "now"):
                        min_date_expr = F.current_date()
                        min_label = "current_date"
                    else:
                        min_date_expr = F.to_date(F.lit(minimum))
                        min_label = str(minimum)
                else:
                    min_date_expr = None

                if min_date_expr is not None and max_date_expr is not None:
                    cond = (date_expr < min_date_expr) | (date_expr > max_date_expr)
                    msg = f"{column} must be between {min_label} and {max_label}"
                elif min_date_expr is not None:
                    cond = date_expr < min_date_expr
                    msg = f"{column} cannot be earlier than {min_label}"
                elif max_date_expr is not None:
                    cond = date_expr > max_date_expr
                    msg = (
                        f"{column} cannot be in the future"
                        if str(maximum).lower() in ("current_date", "today", "now")
                        else f"{column} cannot be later than {max_label}"
                    )
                else:
                    continue

                add_rule("range", F.col(column).isNotNull() & cond, msg)

            else:
                # Numeric / Standard Range
                if minimum is not None and maximum is not None:
                    if inclusive:
                        cond = (F.col(column) < minimum) | (F.col(column) > maximum)
                        msg = f"{column} is outside range [{minimum}, {maximum}]"
                    else:
                        cond = (F.col(column) <= minimum) | (F.col(column) >= maximum)
                        msg = f"{column} is outside range ({minimum}, {maximum})"
                elif minimum is not None and maximum is None:
                    if inclusive:
                        cond = F.col(column) < minimum
                        msg = f"{column} must be >= {minimum}"
                    else:
                        cond = F.col(column) <= minimum
                        msg = f"{column} must be > {minimum}"
                elif minimum is None and maximum is not None:
                    if inclusive:
                        cond = F.col(column) > maximum
                        msg = f"{column} must be <= {maximum}"
                    else:
                        cond = F.col(column) >= maximum
                        msg = f"{column} must be < {maximum}"
                else:
                    continue

                add_rule("range", F.col(column).isNotNull() & cond, msg)

        # Build row-level error messages.
        error_expressions = []

        for conditions in rule_conditions.values():
            for condition, message in conditions:
                error_expressions.append(
                    F.when(condition, F.lit(message))
                )

        errors_column = new_internal_column("_quality_errors")

        if error_expressions:
            df = df.withColumn(
                errors_column,
                F.filter(
                    F.array(*error_expressions),
                    lambda error: error.isNotNull(),
                ),
            )
        else:
            df = df.withColumn(
                errors_column,
                F.array().cast("array<string>"),
            )

        # Build Quality Report timestamp
        timestamp = datetime.now(timezone.utc).isoformat()

        # Separate valid and rejected records.
        rejected_records = (
            df.filter(F.size(F.col(errors_column)) > 0)
            .withColumn("run_id", F.lit(self.run_id))
            .withColumn(
                "rejection_reason",
                F.concat_ws("; ", F.col(errors_column)),
            )
            .withColumn("rejection_timestamp", F.lit(timestamp))
            .drop(errors_column, *internal_columns)
        )

        valid_records = (
            df.filter(F.size(F.col(errors_column)) == 0)
            .drop(errors_column, *internal_columns)
        )

        # 6. Row Count Validation (dataset-level check).
        row_count_failed = (
            min_rows is not None and records_checked < min_rows
        )

        # Build Quality Report.
        report_rows = []

        for rule_name, conditions in rule_conditions.items():
            if conditions:
                combined_condition = conditions[0][0]

                for condition, _ in conditions[1:]:
                    combined_condition = (
                        combined_condition | condition
                    )

                failed_count = (
                    df.filter(combined_condition).count()
                )

                failed_messages = []

                for condition, message in conditions:
                    if df.filter(condition).limit(1).count() > 0:
                        failed_messages.append(message)

                # A schema issue must still be reported if the
                # input DataFrame is empty.
                if rule_name == "schema" and schema_errors:
                    failed_count = max(failed_count, 1)

            else:
                failed_count = 0
                failed_messages = []

            report_rows.append((
                self.run_id,
                self.pipeline_name,
                self.task_name,
                rule_name,
                "FAILED" if failed_count else "PASSED",
                records_checked,
                failed_count,
                None,
                timestamp,
                "; ".join(failed_messages) if failed_messages else None,
            ))

        # Row count is a dataset-level check, not a row-level rejection.
        report_rows.append((
            self.run_id,
            self.pipeline_name,
            self.task_name,
            "row_count",
            "FAILED" if row_count_failed else "PASSED",
            records_checked,
            1 if row_count_failed else 0,
            f"minimum rows: {min_rows}" if min_rows is not None else None,
            timestamp,
            (
                f"Expected at least {min_rows} rows, "
                f"found {records_checked}"
                if row_count_failed else None
            ),
        ))

        report_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("task_name", StringType(), False),
            StructField("check_name", StringType(), False),
            StructField("check_status", StringType(), False),
            StructField("records_checked", LongType(), False),
            StructField("records_failed", LongType(), False),
            StructField("threshold", StringType(), True),
            StructField("execution_timestamp", StringType(), False),
            StructField("error_message", StringType(), True),
        ])

        quality_report = df.sparkSession.createDataFrame(
            report_rows,
            schema=report_schema,
        )

        return valid_records, rejected_records, quality_report

    @staticmethod
    def safe_cast(df, target_schema, date_formats=None):
        """
        Safely cast columns in a DataFrame according to target_schema using try_cast.
        Prevents silent data loss by detecting unparseable values (e.g. non-numeric strings
        being cast to double/bigint) and providing parse error expressions.

        target_schema: dict mapping column name -> type string (e.g. 'bigint', 'double', 'date', 'timestamp')
        date_formats: optional dict mapping column name -> date format string (e.g. 'yyyy-MM-dd')

        Returns: (parsed_df, parse_error_expressions)
        """
        date_formats = date_formats or {}
        parse_error_expressions = []
        result_df = df

        for col_name, target_type in target_schema.items():
            if col_name not in df.columns:
                continue

            current_type = df.schema[col_name].dataType.simpleString().lower()
            target_type_str = str(target_type).lower()

            if current_type == "string" and target_type_str not in ("string", "text"):
                raw_col = F.col(col_name)

                if target_type_str in ("bigint", "long", "int", "integer"):
                    try_expr = F.expr(f"try_cast(`{col_name}` as bigint)")
                    err_msg = f"Cannot parse '{col_name}' as integer"
                elif target_type_str in ("double", "float", "decimal", "numeric"):
                    try_expr = F.expr(f"try_cast(`{col_name}` as double)")
                    err_msg = f"Cannot parse '{col_name}' as numeric"
                elif target_type_str == "date":
                    fmt = date_formats.get(col_name)
                    try_expr = F.to_date(raw_col, fmt) if fmt else F.expr(f"try_cast(`{col_name}` as date)")
                    err_msg = f"Cannot parse '{col_name}' as date"
                elif target_type_str == "timestamp":
                    fmt = date_formats.get(col_name)
                    try_expr = F.to_timestamp(raw_col, fmt) if fmt else F.expr(f"try_cast(`{col_name}` as timestamp)")
                    err_msg = f"Cannot parse '{col_name}' as timestamp"
                else:
                    continue

                invalid_cond = (
                    raw_col.isNotNull()
                    & (F.trim(raw_col) != "")
                    & try_expr.isNull()
                )

                err_col_name = f"_dq_cast_err_{col_name}"
                result_df = (
                    result_df
                    .withColumn(err_col_name, F.when(invalid_cond, F.lit(err_msg)))
                    .withColumn(col_name, try_expr)
                )
                parse_error_expressions.append(F.col(err_col_name))

        return result_df, parse_error_expressions
