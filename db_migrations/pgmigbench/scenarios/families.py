from __future__ import annotations

HOT_RENAME = "hot_column_rename"
ADD_NOT_NULL = "add_non_null_column"
TYPE_NARROW = "type_narrowing"
DROP_LEGACY = "drop_legacy_column"
ADD_FK = "add_foreign_key"

FAMILIES: tuple[str, ...] = (
    HOT_RENAME,
    ADD_NOT_NULL,
    TYPE_NARROW,
    DROP_LEGACY,
    ADD_FK,
)


def family_slug(family: str) -> str:
    return family.replace("_", "")
