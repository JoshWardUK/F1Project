from deltalake import DeltaTable


def arrow_schema_to_map(schema):
    # pyarrow.Schema -> {column_name: type_string}
    return {field.name: str(field.type) for field in schema}


def compare_field_maps(old, new):
    old_keys, new_keys = set(old), set(new)

    added = {k: new[k] for k in new_keys - old_keys}
    removed = {k: old[k] for k in old_keys - new_keys}
    type_changed = {
        k: (old[k], new[k])
        for k in (old_keys & new_keys)
        if old[k] != new[k]
    }

    return added, removed, type_changed


def compare_delta_versions(delta_path: str, old_version: int, new_version: int):
    old_dt = DeltaTable(delta_path, version=old_version)
    new_dt = DeltaTable(delta_path, version=new_version)

    old_schema = arrow_schema_to_map(old_dt.schema().to_pyarrow())
    new_schema = arrow_schema_to_map(new_dt.schema().to_pyarrow())

    added, removed, type_changed = compare_field_maps(old_schema, new_schema)

    # 👇 Explicit no-change case
    if not added and not removed and not type_changed:
        print(
            f"✅ No schema changes detected "
            f"(Delta versions {old_version} → {new_version})"
        )
        return {
            "status": "no_change",
            "added": {},
            "removed": {},
            "type_changed": {},
        }

    # 👇 Changes detected
    print(f"⚠️ Schema changes detected (Delta versions {old_version} → {new_version})")

    if added:
        print("  ➕ Added columns:")
        for k, v in added.items():
            print(f"    - {k}: {v}")

    if removed:
        print("  ➖ Removed columns:")
        for k, v in removed.items():
            print(f"    - {k}: {v}")

    if type_changed:
        print("  🔄 Type changes:")
        for k, (old_t, new_t) in type_changed.items():
            print(f"    - {k}: {old_t} → {new_t}")

    return {
        "status": "changed",
        "added": added,
        "removed": removed,
        "type_changed": type_changed,
    }