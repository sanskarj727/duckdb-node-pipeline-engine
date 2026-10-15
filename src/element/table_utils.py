from uuid import uuid4


def make_table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"
