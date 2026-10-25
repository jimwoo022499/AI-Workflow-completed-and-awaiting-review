# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup and seed (fictional data only)
# MAGIC Creates the Unity Catalog schema, the Delta tables, and the Lakebase Postgres outbox tables
# MAGIC (`idempotency_key` has a **UNIQUE** constraint), then seeds fictional startups.
# MAGIC Re-running resets the demo.

# COMMAND ----------

# MAGIC %pip install -q "pg8000>=1.31" "databricks-sdk>=0.81" "mlflow-skinny>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./lib_workflow

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ} COMMENT 'Program ops showcase (fictional data)'")

DDL = {
    "founders": "startup_id STRING NOT NULL, startup_name STRING, domain STRING, founder_name STRING, founder_email STRING",
    "program_tracker": "record_id STRING NOT NULL, startup_id STRING, status STRING, stage STRING, updated_at TIMESTAMP, updated_by STRING, updated_by_type STRING",
    "review_queue": "record_id STRING NOT NULL, startup_id STRING, status STRING, updated_at TIMESTAMP, updated_by STRING, updated_by_login STRING, updated_by_type STRING",
    "inbound_emails": "message_id STRING NOT NULL, from_email STRING, to_email STRING, subject STRING, body STRING, received_at TIMESTAMP, delivered_count INT, intent STRING, sender_verified BOOLEAN, claimed_startup_id STRING, case_id STRING, run_context STRING",
    "cases": "case_id STRING NOT NULL, startup_id STRING, message_id STRING, run_context STRING, opened_at TIMESTAMP, case_state STRING, sender_verified BOOLEAN, needs_human_reason STRING, reconciliation STRING, suggested_reading STRING, evidence STRING, hold_placed BOOLEAN, response_deadline TIMESTAMP, escalated_to STRING, decision_version INT, final_status STRING, resolved_by STRING, updated_at TIMESTAMP",
    "case_summaries": "summary_id STRING, case_id STRING, generated_at TIMESTAMP, model_endpoint STRING, prompt STRING, ai_raw STRING, validation_passed BOOLEAN, validation_errors STRING, version_used STRING, summary_json STRING, mlflow_trace_id STRING",
    "reviewer_decisions": "decision_id STRING, case_id STRING, decision_version INT, reviewer_name STRING, reviewer_login STRING, decided_at TIMESTAMP, final_status STRING, records_before STRING, records_after STRING, reason STRING, draft_subject STRING, draft_body STRING",
    "audit_log": "event_id STRING, seq BIGINT, case_id STRING, startup_id STRING, run_context STRING, event_time TIMESTAMP, recorded_at TIMESTAMP, event_type STRING, actor STRING, actor_type STRING, headline STRING, details STRING",
    "test_results": "run_id STRING, test_no INT, test_name STRING, passed BOOLEAN, assertions STRING, details STRING, ran_at TIMESTAMP",
}
for t, cols in DDL.items():
    spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t}")
    props = "TBLPROPERTIES ('delta.appendOnly' = 'true')" if t == "audit_log" else ""
    spark.sql(f"CREATE TABLE {FQ}.{t} ({cols}) {props}")

# Informational-only constraints in Delta (documented in README): NOT enforced for uniqueness.
spark.sql(f"ALTER TABLE {FQ}.inbound_emails ADD CONSTRAINT inbound_pk PRIMARY KEY (message_id)")
spark.sql(f"ALTER TABLE {FQ}.cases ADD CONSTRAINT cases_pk PRIMARY KEY (case_id)")
spark.sql(f"COMMENT ON TABLE {FQ}.audit_log IS 'Append-only event log: every workflow step (delta.appendOnly=true)'")
print("Delta tables created in", FQ)

# COMMAND ----------

# MAGIC %md ### Fictional seed data

# COMMAND ----------

def T(s): return datetime.fromisoformat(s)

founders = [
    ("STU-0427", "Soul Labs", "soul.example", "Tommy Lam", "tommy@soul.example"),
    ("STU-0311", "Nimbus Forge", "nimbusforge.example", "Priya Nair", "priya@nimbusforge.example"),
    ("STU-0388", "Kelp Robotics", "kelp.example", "Marco Ruiz", "marco@kelp.example"),
    ("STU-0402", "Quill Analytics", "quill.example", "Ada Okafor", "ada@quill.example"),
]
tracker = [
    ("PT-0427-01", "STU-0427", "Completed", None, T("2026-10-14 08:00:00"), "sync-bot", "system"),
    ("PT-0311-01", "STU-0311", "Approved", "Cohort 7", T("2026-10-10 08:00:00"), "sync-bot", "system"),
    ("PT-0388-01", "STU-0388", "In review", "Technical diligence", T("2026-10-16 08:00:00"), "sync-bot", "system"),
    ("PT-0402-01", "STU-0402", "Approved", "Cohort 7", T("2026-10-12 08:00:00"), "sync-bot", "system"),
]
queue = [
    ("RQ-0427-01", "STU-0427", "Awaiting review", T("2026-10-19 14:30:00"), "Jimmy Woo", "j.woo", "human"),
    ("RQ-0311-01", "STU-0311", "Approved", T("2026-10-09 16:10:00"), "Jimmy Woo", "j.woo", "human"),
    ("RQ-0388-01", "STU-0388", "Awaiting review", T("2026-10-17 11:45:00"), "Dana Reyes", "d.reyes", "human"),
    ("RQ-0402-01", "STU-0402", "Approved", T("2026-10-11 10:20:00"), "Jimmy Woo", "j.woo", "human"),
]
spark.createDataFrame(founders, DDL["founders"]).write.mode("append").saveAsTable(f"{FQ}.founders")
spark.createDataFrame(tracker, DDL["program_tracker"]).write.mode("append").saveAsTable(f"{FQ}.program_tracker")
spark.createDataFrame(queue, DDL["review_queue"]).write.mode("append").saveAsTable(f"{FQ}.review_queue")
display(spark.sql(f"""SELECT f.startup_id, f.startup_name, f.founder_email, t.status AS tracker_status, t.stage,
                             q.status AS queue_status, q.updated_by AS queue_updated_by
                      FROM {FQ}.founders f LEFT JOIN {FQ}.program_tracker t USING (startup_id)
                      LEFT JOIN {FQ}.review_queue q USING (startup_id) ORDER BY 1"""))

# COMMAND ----------

# MAGIC %md ### Lakebase Postgres: outbox with a real UNIQUE constraint

# COMMAND ----------

with pg_connect() as conn:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}")
    conn.execute(f"DROP TABLE IF EXISTS {PG_SCHEMA}.outbox, {PG_SCHEMA}.outbox_attempts, {PG_SCHEMA}.provider_deliveries")
    conn.execute(f"""CREATE TABLE {PG_SCHEMA}.outbox (
        outbox_id        BIGSERIAL PRIMARY KEY,
        idempotency_key  TEXT NOT NULL,
        case_id          TEXT NOT NULL,
        email_type       TEXT NOT NULL,
        decision_version INT,
        recipient        TEXT NOT NULL,
        subject          TEXT,
        body             TEXT,
        state            TEXT NOT NULL CHECK (state IN ('PENDING','SENT','FAILED','UNKNOWN')),
        provider_message_id TEXT,
        created_at       TIMESTAMP NOT NULL,
        updated_at       TIMESTAMP NOT NULL,
        last_error       TEXT,
        CONSTRAINT outbox_idempotency_key_uq UNIQUE (idempotency_key))""")
    conn.execute(f"""CREATE TABLE {PG_SCHEMA}.outbox_attempts (
        attempt_id      BIGSERIAL PRIMARY KEY,
        idempotency_key TEXT NOT NULL,
        case_id         TEXT,
        email_type      TEXT,
        attempt_label   TEXT,
        attempted_at    TIMESTAMP NOT NULL,
        outcome         TEXT NOT NULL CHECK (outcome IN ('CLAIMED','SENT','FAILED','UNKNOWN','STOPPED_DUPLICATE','BLOCKED_BY_GATE')),
        detail          TEXT)""")
    conn.execute(f"""CREATE TABLE {PG_SCHEMA}.provider_deliveries (
        provider_message_id TEXT PRIMARY KEY,
        idempotency_key     TEXT,
        recipient           TEXT,
        subject             TEXT,
        delivered_at        TIMESTAMP)""")
    cons = conn.execute("""SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
                           WHERE conrelid = 'program_ops.outbox'::regclass""").fetchall()
print("Lakebase outbox constraints:")
for c in cons:
    print("  ", c[0], "->", c[1])
assert any("UNIQUE (idempotency_key)" in c[1] for c in cons)
sync_outbox_mirror()
