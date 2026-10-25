# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Reviewer console: the program lead decides
# MAGIC Jimmy Woo (program lead) is alerted by email. He opens this notebook, reads the evidence, chooses **APPROVED** or
# MAGIC **REJECTED**, gives a reason and sets **action = submit**. The decision is written to `reviewer_decisions` and
# MAGIC `audit_log`, both source records are updated to match, and the approval or rejection email is drafted.
# MAGIC Nothing is ever decided or sent automatically.
# MAGIC
# MAGIC *In the demo job this notebook runs as a task, with widget values standing in for Jimmy Woo's clicks.*

# COMMAND ----------

# MAGIC %pip install -q "pg8000>=1.31" "databricks-sdk>=0.81" "mlflow-skinny>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./lib_workflow

# COMMAND ----------

open_cases = [r["case_id"] for r in rows(f"SELECT case_id FROM {FQ}.cases WHERE case_state = 'AWAITING_DECISION' ORDER BY run_context, opened_at DESC")]
dbutils.widgets.text("case_id", open_cases[0] if open_cases else "", "Case ID")
dbutils.widgets.text("reviewer_name", "Jimmy Woo", "Program lead")
dbutils.widgets.text("reviewer_login", "j.woo", "Reviewer login")
dbutils.widgets.dropdown("final_status", "APPROVED", ["APPROVED", "REJECTED"], "Decision")
dbutils.widgets.text("reason", "", "Reason (required)")
dbutils.widgets.text("decided_at", "", "Decision time (blank = now)")
dbutils.widgets.dropdown("action", "preview", ["preview", "submit"], "Action")

case_id = dbutils.widgets.get("case_id")
try:
    case_id = dbutils.jobs.taskValues.get(taskKey="story_intake", key="case_id", debugValue=case_id) or case_id
except Exception:
    pass
c = get_case(case_id)
assert c, f"case {case_id} not found"
recon = json.loads(c["evidence"]) if c["evidence"] else {}

# COMMAND ----------

# MAGIC %md ## Evidence

# COMMAND ----------

print(f"Case {case_id} | startup {c['startup_id']} | state {c['case_state']} | reconciliation {c['reconciliation']}")
print(f"Response deadline: {c['response_deadline']} UTC | escalated to: {c['escalated_to']}")
print("Suggested reading (code, not a decision):", c["suggested_reading"])
print("What you need to decide:", LEAD_ACTION.get(c["reconciliation"], "Review the case."))
display(spark.createDataFrame([{k: str(v) for k, v in e.items()} for e in recon.get("evidence", [])]))
display(spark.createDataFrame([{k: str(v) for k, v in d.items()} for d in recon.get("diff", [])]))

# COMMAND ----------

s = rows(f"SELECT * FROM {FQ}.case_summaries WHERE case_id = :c ORDER BY generated_at DESC LIMIT 1", c=case_id)
if s:
    s = s[0]
    summ = json.loads(s["summary_json"])
    print(f"Summary version used: {s['version_used']} | validation passed: {s['validation_passed']} | model: {s['model_endpoint']}")
    print("\nKNOWN")
    for k in summ["known"]:
        print(f"  - {k['claim']}  [{k['source_table']}.{k['source_column']} = {k['value']}]")
    print("\nUNKNOWN")
    for u in summ["unknown"]:
        print(f"  - {u['question']}  ({u['why_unknown']})")

# COMMAND ----------

display(spark.sql(f"SELECT event_time, event_type, actor, headline FROM {FQ}.audit_log WHERE case_id = '{case_id}' ORDER BY event_time, seq"))

# COMMAND ----------

# MAGIC %md ## Decision

# COMMAND ----------

if dbutils.widgets.get("action") == "submit":
    reason = dbutils.widgets.get("reason").strip()
    assert reason, "A reason is required"
    decided_at = dbutils.widgets.get("decided_at").strip()
    v = record_reviewer_decision(case_id, dbutils.widgets.get("reviewer_name"), dbutils.widgets.get("reviewer_login"),
                                 dbutils.widgets.get("final_status"), reason,
                                 now=ts(decided_at) if decided_at else None, ctx=c["run_context"])
    d = rows(f"SELECT * FROM {FQ}.reviewer_decisions WHERE case_id = :c ORDER BY decided_at DESC LIMIT 1", c=case_id)[0]
    print(f"Decision recorded: {d['final_status']} (decision_version {v}). Both records now say the same thing.")
    print("\nDRAFT EMAIL TO THE FOUNDER (sent once by the next step)")
    print("Subject:", d["draft_subject"])
    print(d["draft_body"])
else:
    print("Preview only. Set action = submit to record the decision.")
