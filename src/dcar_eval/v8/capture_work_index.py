"""One exact additive index may accompany the immutable schema23 migration.

This module neither executes DDL nor rewrites a historical migration receipt.
All other schema objects remain in the original complete-object hash check.
"""

INDEX_NAME = "idx_capture_work_content_cycle"
INDEX_SQL = "CREATE INDEX idx_capture_work_content_cycle ON capture_work_items(content_id,operation,state,account_id) WHERE content_id IS NOT NULL"
INDEX_OBJECT = ("index", INDEX_NAME, "capture_work_items", INDEX_SQL)


def normalize_objects(items: list[tuple]) -> list[tuple]:
    result = []
    present = False
    for value in items:
        item = tuple(value)
        if len(item) != 4:
            raise ValueError("schema object shape differs")
        if item[1] == INDEX_NAME:
            if present or item != INDEX_OBJECT:
                raise ValueError("capture work index differs from its exact approved object")
            present = True
            continue
        result.append(item)
    return result
