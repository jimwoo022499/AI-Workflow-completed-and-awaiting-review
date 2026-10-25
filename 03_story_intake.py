# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Story, part 1: Tommy's email arrives
# MAGIC Simulated clock: Saturday 2026-10-24, 09:02 UTC.
# MAGIC 1. Check the sender matches the founder on file for STU-0427.
# MAGIC 2. Send the founder an acknowledgement: "we are now reviewing the status of your inquiry, reply within 2 business days".
# MAGIC 3. Reconcile both records (CONFLICT) and write a fact-checked AI summary of what is known and unknown.
# MAGIC 4. Put the decision email on hold and alert the program lead, Jimmy Woo.

# COMMAND ----------

# MAGIC %pip install -q "pg8000>=1.31" "databricks-sdk>=0.81" "mlflow-skinny>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./lib_workflow

# COMMAND ----------

TOMMY_EMAIL = {
    "message_id": "<20261024090200.soul0427@soul.example>",
    "from_email": "tommy@soul.example",
    "to_email": "programs@accelerator.example",
    "subject": "Quick question",
    "body": "Hi team, are we approved?",
    "received_at": "2026-10-24 09:02:00",
}
result = process_inbound(TOMMY_EMAIL, ctx="STORY")
case_id = result["case_id"]
print(result)
dbutils.jobs.taskValues.set(key="case_id", value=case_id)

# COMMAND ----------

c = get_case(case_id)
print(f"Case {case_id}: state={c['case_state']} reconciliation={c['reconciliation']} hold={c['hold_placed']}")
print("Suggested reading:", c["suggested_reading"])
display(spark.sql(f"SELECT summary_json, validation_passed, version_used, validation_errors FROM {FQ}.case_summaries WHERE case_id = '{case_id}'"))
assert c["reconciliation"] == "CONFLICT" and c["case_state"] == "AWAITING_DECISION"
