"""Silver layer: typed, validated, current-state tables built from bronze.

The SQL in ``sql/30_silver_reference.sql`` and ``sql/40_silver_events.sql`` is generated from the
table specs in ``spec.py`` by ``render.py``, so the rules are written once and cannot drift between
tables. ``local.py`` runs the same SQL on DuckDB so the logic can be tested without Snowflake.
"""
