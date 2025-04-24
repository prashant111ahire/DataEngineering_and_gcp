from pyspark.sql.functions import col, count, when, isnan

def null_check(df, columns, threshold):
    results = {}
    total = df.count()
    for col_name in columns:
        null_frac = df.filter(col(col_name).isNull() | isnan(col(col_name))).count() / total
        results[col_name] = {
            "null_percentage": null_frac,
            "status": "FAIL" if null_frac > threshold else "PASS"
        }
    return results

def duplicate_check(df, columns):
    count_all = df.count()
    count_dedup = df.dropDuplicates(columns).count()
    has_dups = count_all != count_dedup
    return {
        "duplicate_check": {
            "duplicates_found": count_all - count_dedup,
            "status": "FAIL" if has_dups else "PASS"
        }
    }

def value_range_check(df, column, min_val, max_val):
    out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
    return {
        column: {
            "out_of_range": out_of_range,
            "status": "FAIL" if out_of_range > 0 else "PASS"
        }
    }

def distinct_count_check(df, column):
    count_distinct = df.select(column).distinct().count()
    return {
        column: {
            "distinct_count": count_distinct,
            "status": "PASS"
        }
    }
