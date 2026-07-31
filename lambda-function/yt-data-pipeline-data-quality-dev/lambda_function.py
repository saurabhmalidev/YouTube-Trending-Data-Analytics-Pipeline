"""
Lambda: Data Quality Checks — no external dependencies, no layer required.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")
athena_client = boto3.client("athena")

SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
ATHENA_S3_OUTPUT = os.environ.get("ATHENA_S3_OUTPUT", "")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = 50_000_000_000
FRESHNESS_HOURS = 48

CRITICAL_COLUMNS = {
    "clean_statistics": ["video_id", "title", "channel_title", "views", "region"],
    "clean_reference_data": ["id", "region"],
}


def run_athena_query(query, database, s3_output, workgroup="primary"):
    """Run an Athena query and return list of dict rows — no pandas."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://{s3_output}/dq-checks/"},
        WorkGroup=workgroup,
    )
    query_id = response["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
        raise Exception(f"Athena query failed: {reason}")

    raw_rows = []
    next_token = None
    while True:
        kwargs = {"QueryExecutionId": query_id}
        if next_token:
            kwargs["NextToken"] = next_token
        results = athena_client.get_query_results(**kwargs)
        raw_rows.extend(results["ResultSet"]["Rows"])
        next_token = results.get("NextToken")
        if not next_token:
            break

    if not raw_rows:
        return []

    headers = [col.get("VarCharValue", "") for col in raw_rows[0]["Data"]]
    rows = []
    for row in raw_rows[1:]:
        values = [col.get("VarCharValue") for col in row["Data"]]
        rows.append(dict(zip(headers, values)))

    return rows


def check_row_count(rows, table_name):
    count = len(rows)
    passed = count >= MIN_ROW_COUNT
    return {
        "check": "row_count", "table": table_name, "value": count,
        "threshold": MIN_ROW_COUNT, "passed": passed,
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
    }


def check_null_percentage(rows, table_name):
    results = []
    cols = CRITICAL_COLUMNS.get(table_name, [])
    total = len(rows)

    if total == 0:
        for col in cols:
            results.append({
                "check": "null_pct", "table": table_name, "column": col,
                "value": 0, "threshold": MAX_NULL_PCT, "passed": True,
                "message": "No rows to check",
            })
        return results

    available_cols = rows[0].keys() if rows else []
    for col in cols:
        if col not in available_cols:
            results.append({
                "check": "null_pct", "table": table_name, "column": col,
                "passed": False, "message": f"Column '{col}' missing from table",
            })
            continue
        null_count = sum(1 for r in rows if r.get(col) is None or r.get(col) == "")
        null_pct = (null_count / total) * 100
        passed = null_pct <= MAX_NULL_PCT
        results.append({
            "check": "null_pct", "table": table_name, "column": col,
            "value": round(null_pct, 2), "threshold": MAX_NULL_PCT, "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
        })
    return results


def check_schema(rows, table_name):
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(rows[0].keys()) if rows else set()
    missing = expected - actual
    passed = len(missing) == 0
    return {
        "check": "schema", "table": table_name, "missing_columns": list(missing),
        "passed": passed,
        "message": f"Missing columns: {missing}" if missing else "All expected columns present",
    }


def check_value_ranges(rows, table_name):
    results = []
    if table_name != "clean_statistics":
        return results

    if rows and "views" in rows[0]:
        negative = 0
        extreme = 0
        for r in rows:
            try:
                v = float(r.get("views") or 0)
                if v < 0:
                    negative += 1
                if v > MAX_VIEWS:
                    extreme += 1
            except (ValueError, TypeError):
                continue
        passed = negative == 0 and extreme == 0
        results.append({
            "check": "value_range", "table": table_name, "column": "views",
            "negative_count": negative, "extreme_count": extreme, "passed": passed,
            "message": f"Views: {negative} negative, {extreme} extreme (>{MAX_VIEWS})",
        })
    return results


def check_freshness(rows, table_name):
    ts_col = None
    if rows:
        if "_processed_at" in rows[0]:
            ts_col = "_processed_at"
        elif "_ingestion_timestamp" in rows[0]:
            ts_col = "_ingestion_timestamp"

    if not ts_col:
        return {
            "check": "freshness", "table": table_name, "passed": True,
            "message": "No timestamp column found — skipping freshness check (backfill data)",
        }

    try:
        timestamps = []
        for r in rows:
            val = r.get(ts_col)
            if val:
                timestamps.append(datetime.fromisoformat(val.replace("Z", "+00:00")))

        if not timestamps:
            return {
                "check": "freshness", "table": table_name, "passed": True,
                "message": "No valid timestamps found — skipping",
            }

        latest = max(timestamps)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        passed = latest >= cutoff
        return {
            "check": "freshness", "table": table_name, "latest_record": str(latest),
            "cutoff": str(cutoff), "passed": passed,
            "message": f"Latest: {latest}, Cutoff: {cutoff}",
        }
    except Exception as e:
        return {
            "check": "freshness", "table": table_name, "passed": True,
            "message": f"Could not parse timestamps: {e} — skipping",
        }


def lambda_handler(event, context):
    database = event.get("database", "yt-pipeline-silver_db")
    tables = event.get("tables", ["clean_statistics"])

    all_results = []
    overall_passed = True

    for table_name in tables:
        logger.info(f"Running DQ checks on {database}.{table_name}...")

        try:
            query = f'SELECT * FROM "{table_name}" LIMIT 10000'
            rows = run_athena_query(
                query=query,
                database=database,
                s3_output=ATHENA_S3_OUTPUT,
                workgroup=ATHENA_WORKGROUP,
            )
        except Exception as e:
            logger.error(f"Could not read {table_name}: {e}")
            all_results.append({
                "check": "read_table", "table": table_name,
                "passed": False, "message": str(e),
            })
            overall_passed = False
            continue

        checks = []
        checks.append(check_row_count(rows, table_name))
        checks.extend(check_null_percentage(rows, table_name))
        checks.append(check_schema(rows, table_name))
        checks.extend(check_value_ranges(rows, table_name))
        checks.append(check_freshness(rows, table_name))

        for check in checks:
            logger.info(f"  {check['check']}: {'PASS' if check['passed'] else 'FAIL'} — {check['message']}")
            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    passed_count = sum(1 for r in all_results if r["passed"])
    total_count = len(all_results)
    logger.info(f"DQ Summary: {passed_count}/{total_count} checks passed. Overall: {'PASS' if overall_passed else 'FAIL'}")

    if not overall_passed and SNS_TOPIC:
        failed = [r for r in all_results if not r["passed"]]
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject="[YT Pipeline] Data quality checks FAILED",
            Message=json.dumps(failed, indent=2, default=str),
        )

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "details": json.loads(json.dumps(all_results, default=str)),
    }
