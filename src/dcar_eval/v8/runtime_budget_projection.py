"""A covering index for the existing live budget calculation, without a cache.

The budget reader needs four metadata fields; provider receipts can be several
kilobytes each. SQLite maintains their typed projection atomically with every
usage write. All rows and amounts still pass the original Python accounting
rules, including historical invalid amounts and malformed metadata.
"""
INDEX_NAME = "idx_provider_usage_budget_projection_v24"
METADATA_SQL = """CASE
 WHEN details_json IS NULL OR details_json='' THEN '{}'
 WHEN json_valid(details_json) THEN CASE WHEN json_type(details_json)='object'
   THEN json_object('budget_day',json_extract(details_json,'$.budget_day'),
     'state',json_extract(details_json,'$.state'),
     'category',json_extract(details_json,'$.category'),
     'budget_bucket',json_extract(details_json,'$.budget_bucket'))
   ELSE NULL END
 ELSE NULL END"""
DDL = (f"CREATE INDEX {INDEX_NAME} ON provider_usage "
       f"(lower(provider),currency,id,amount,recorded_at,operation,({METADATA_SQL}))")
SELECT_SQL = (f"SELECT id,amount,recorded_at,operation,({METADATA_SQL}) AS details_json "
              f"FROM provider_usage INDEXED BY {INDEX_NAME} "
              "WHERE lower(provider)='tikhub' AND currency='USD'")


def rows(connection):
    return connection.execute(SELECT_SQL)
