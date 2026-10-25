# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Story, part 2: the drafted decision email is sent once, the retry is stopped
# MAGIC 10:06: the approval email drafted after Jimmy Woo's decision passes the send gate and is SENT.
# MAGIC 10:13: a simulated retry (e.g. a re-run job task) tries to send the same decision again and is **STOPPED_DUPLICATE** by
# MAGIC the Lakebase UNIQUE constraint on `idempotency_key`.

# COMMAND ----------

# MAGIC %pip install -q "pg8000>=1.31" "databricks-sdk>=0.81" "mlflow-skinny>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./lib_workflow

# COMMAND ----------

case_id = dbutils.jobs.taskValues.get(taskKey="story_intake", key="case_id", debugValue=case_id_for("<20261024090200.soul0427@soul.example>"))
first = send_email(case_id, "DECISION", "workflow: send after decision", now=ts("2026-10-24 10:06:00"))
retry = send_email(case_id, "DECISION", "simulated retry at 10:13", now=ts("2026-10-24 10:13:00"))
print("10:06 ->", first)
print("10:13 ->", retry)
assert first["outcome"] == "SENT" and retry["outcome"] == "STOPPED_DUPLICATE"
check_deadlines(now=ts("2026-10-24 10:14:00"))  # nothing overdue; shown for completeness
sync_outbox_mirror()

# COMMAND ----------

# MAGIC %md ## Reviewer evidence: `case_timeline` for STU-0427

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.case_timeline
COMMENT 'Per-case evidence in time order: sender check, acknowledgement, records side by side, diff, AI summary + validation, lead alert, decision, draft, outbox log'
AS
WITH events AS (
  SELECT case_id, startup_id, run_context, event_time, seq, 'audit_log' AS source, event_type, actor, actor_type,
         headline, details
  FROM {FQ}.audit_log
  UNION ALL
  SELECT a.case_id, c.startup_id, c.run_context, a.attempted_at, a.attempt_id, 'lakebase.outbox_attempts',
         concat('OUTBOX_', a.outcome), a.attempt_label, 'system',
         concat(a.email_type, ' attempt "', a.attempt_label, '": ', a.outcome, coalesce(concat(' (', a.detail, ')'), '')),
         to_json(named_struct('idempotency_key', a.idempotency_key, 'outbox_state_now', o.state,
                              'provider_message_id', o.provider_message_id))
  FROM {FQ}.outbox_attempts a
  JOIN {FQ}.cases c ON c.case_id = a.case_id
  LEFT JOIN {FQ}.outbox_log o ON o.idempotency_key = a.idempotency_key
)
SELECT case_id, startup_id, run_context,
       row_number() OVER (PARTITION BY case_id ORDER BY event_time, seq) AS step,
       event_time, source, event_type, actor, actor_type, headline, details
FROM events
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.case_records_side_by_side AS
SELECT f.startup_id, f.startup_name, f.founder_email,
       t.record_id AS tracker_record, t.status AS tracker_status, t.stage AS tracker_stage, t.updated_at AS tracker_updated_at,
       t.updated_by AS tracker_updated_by, t.updated_by_type AS tracker_edit_kind,
       q.record_id AS queue_record, q.status AS queue_status, q.updated_at AS queue_updated_at,
       q.updated_by AS queue_updated_by, q.updated_by_type AS queue_edit_kind
FROM {FQ}.founders f
LEFT JOIN {FQ}.program_tracker t USING (startup_id)
LEFT JOIN {FQ}.review_queue q USING (startup_id)
""")

timeline = spark.sql(f"""SELECT step, event_time, event_type, actor, actor_type, headline
                         FROM {FQ}.case_timeline WHERE case_id = '{case_id}' ORDER BY step""")
display(timeline)
for r in timeline.collect():
    print(f"{r.step:>2} {r.event_time:%H:%M} {r.event_type:<32} {r.actor_type:<8} {r.headline}")
