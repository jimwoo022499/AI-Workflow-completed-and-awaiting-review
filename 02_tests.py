# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Tests
# MAGIC Each test runs the real workflow code against the Delta tables and the Lakebase outbox, then checks the
# MAGIC results with assertions. Results go to `test_results`. Test cases are tagged `run_context = 'TEST'`.

# COMMAND ----------

# MAGIC %pip install -q "pg8000>=1.31" "databricks-sdk>=0.81" "mlflow-skinny>=3.1"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./lib_workflow

# COMMAND ----------

import traceback
from concurrent.futures import ThreadPoolExecutor

RUN_ID = uuid.uuid4().hex[:8]
results = []

def msg(n, frm, body, subject="Question", startup_hint=""):
    return {"message_id": f"<test{n}-{RUN_ID}@mail.example>", "from_email": frm,
            "subject": f"{subject} {startup_hint}".strip(), "body": body, "received_at": utcnow()}

def outbox_for(case_id, email_type=None):
    q = f"SELECT * FROM {PG_SCHEMA}.outbox WHERE case_id = %s" + (" AND email_type = %s" if email_type else "")
    return pg_rows(q, (case_id, email_type) if email_type else (case_id,))

def deliveries_for(key):
    return pg_rows(f"SELECT * FROM {PG_SCHEMA}.provider_deliveries WHERE idempotency_key = %s", (key,))

def attempts_for(case_id):
    return pg_rows(f"SELECT * FROM {PG_SCHEMA}.outbox_attempts WHERE case_id = %s ORDER BY attempt_id", (case_id,))

def run_test(no, name, fn):
    checks, details = [], {}
    def check(label, ok, observed=None):
        checks.append({"assertion": label, "passed": bool(ok), "observed": observed})
    try:
        details = fn(check) or {}
    except Exception as e:
        check("test ran without exception", False, f"{type(e).__name__}: {e}")
        details = {"traceback": traceback.format_exc()[-2000:]}
    passed = bool(checks) and all(c["passed"] for c in checks)
    results.append((RUN_ID, no, name, passed, jdump(checks), jdump(details), utcnow()))
    print(f"{'PASS' if passed else 'FAIL'}  {no}. {name}")
    for c in checks:
        print(f"      [{'x' if c['passed'] else ' '}] {c['assertion']}  -> {c['observed']}")

# COMMAND ----------

state = {}
LEAD = PROGRAM_LEAD["email"]

def t1(check):
    r = process_inbound(msg(1, "tommy@soul.example", "Hi team, are we approved?"), ctx="TEST")
    cid = r["case_id"]; state["t1_case"] = cid
    early = send_email(cid, "DECISION", "test: decision email before the lead decides", ctx="TEST")
    c = get_case(cid)
    ack = outbox_for(cid, "ACKNOWLEDGEMENT")
    alert = outbox_for(cid, "LEAD_ALERT")
    check("reconciliation = CONFLICT", c["reconciliation"] == "CONFLICT", c["reconciliation"])
    check("acknowledgement SENT once to the founder, no status words", len(ack) == 1 and ack[0]["state"] == "SENT"
          and ack[0]["recipient"] == "tommy@soul.example" and not STATUS_WORDS.search(ack[0]["body"]), [a["state"] for a in ack])
    check("program lead alerted", len(alert) == 1 and alert[0]["recipient"] == LEAD and alert[0]["state"] == "SENT",
          [a["recipient"] for a in alert])
    check("case waits for the lead (AWAITING_DECISION, on hold)", c["case_state"] == "AWAITING_DECISION" and c["hold_placed"],
          (c["case_state"], c["hold_placed"]))
    check("decision email blocked until the lead decides", early["outcome"] == "BLOCKED_BY_GATE"
          and not outbox_for(cid, "DECISION"), early["outcome"])
    return {"case_id": cid, "acknowledgement": ack[0]["body"] if ack else None}

def t2(check):
    r = process_inbound(msg(2, "priya@nimbusforge.example", "Hi, could you confirm our program status?"), ctx="TEST")
    cid = r["case_id"]
    before = outbox_for(cid, "DECISION")
    record_reviewer_decision(cid, "Jimmy Woo", "j.woo", "APPROVED", "Both records say approved; confirmed.", ctx="TEST")
    first = send_email(cid, "DECISION", "workflow", ctx="TEST")
    again = send_email(cid, "DECISION", "test: repeat send", ctx="TEST")
    ob = outbox_for(cid, "DECISION")
    check("records agree, but nothing sent before the lead decides", r["state"] == "AWAITING_DECISION" and not before, r["state"])
    check("approval email drafted and SENT exactly once", len(ob) == 1 and ob[0]["state"] == "SENT"
          and "approved" in ob[0]["subject"].lower(), [o["state"] for o in ob])
    check("provider message id stored", bool(ob and ob[0]["provider_message_id"]), ob[0]["provider_message_id"] if ob else None)
    check("provider received exactly one delivery", len(deliveries_for(ob[0]["idempotency_key"])) == 1, None)
    check("repeat send STOPPED_DUPLICATE", first["outcome"] == "SENT" and again["outcome"] == "STOPPED_DUPLICATE", again["outcome"])
    return {"case_id": cid}

def t3(check):
    m = msg(3, "tommy@soul.example", "Hi team, are we approved?")
    r1 = process_inbound(dict(m), ctx="TEST")
    r2 = process_inbound(dict(m), ctx="TEST")
    n_cases = rows(f"SELECT count(*) n FROM {FQ}.cases WHERE message_id = :m", m=m["message_id"])[0]["n"]
    inbound = rows(f"SELECT count(*) n, max(delivered_count) d FROM {FQ}.inbound_emails WHERE message_id = :m", m=m["message_id"])[0]
    check("exactly one case for the message_id", n_cases == 1, n_cases)
    check("second delivery flagged as duplicate, same case", r2.get("duplicate") and r2["case_id"] == r1["case_id"], r2)
    check("one inbound row, delivered_count = 2", inbound["n"] == 1 and inbound["d"] == 2, inbound)
    check("only one acknowledgement and one lead alert", len(outbox_for(r1["case_id"], "ACKNOWLEDGEMENT")) == 1
          and len(outbox_for(r1["case_id"], "LEAD_ALERT")) == 1, None)
    return {"case_id": r1["case_id"]}

def t4(check):
    r = process_inbound(msg(4, "marco@kelp.example", "Any update on whether we got in?"), ctx="TEST")
    cid = r["case_id"]
    record_reviewer_decision(cid, "Jimmy Woo", "j.woo", "REJECTED", "Diligence did not meet the bar for this cohort.", ctx="TEST")
    draft = rows(f"SELECT draft_subject, draft_body FROM {FQ}.reviewer_decisions WHERE case_id = :c", c=cid)[0]
    send_email(cid, "DECISION", "workflow", ctx="TEST", provider_mode="timeout")
    ob = outbox_for(cid, "DECISION")
    sweep = retry_sweeper(ctx="TEST")
    ob_after = outbox_for(cid, "DECISION")
    claims = [a for a in attempts_for(cid) if a["outcome"] == "CLAIMED" and a["email_type"] == "DECISION"]
    c = get_case(cid)
    check("rejection email drafted after the lead rejected", "unable to offer" in draft["draft_body"], draft["draft_subject"])
    check("outbox state UNKNOWN after timeout", len(ob) == 1 and ob[0]["state"] == "UNKNOWN", [o["state"] for o in ob])
    check("retry sweeper skipped it", any(s["case_id"] == cid and s["action"].startswith("skipped") for s in sweep), None)
    check("still UNKNOWN after sweeper (no auto-retry)", ob_after[0]["state"] == "UNKNOWN", ob_after[0]["state"])
    check("provider called exactly once", len(claims) == 1 and len(deliveries_for(ob[0]["idempotency_key"])) == 1, len(claims))
    check("case routed to a human", bool(c["needs_human_reason"]), c["needs_human_reason"])
    return {"case_id": cid}

def t5(check):
    r = process_inbound(msg(5, "ada@quill.example", "Hello, are we confirmed for the cohort?"), ctx="TEST")
    cid = r["case_id"]
    record_reviewer_decision(cid, "Jimmy Woo", "j.woo", "APPROVED", "Confirmed approval.", ctx="TEST")
    barrier = threading.Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(send_email, cid, "DECISION", label, None, "ok", "TEST", barrier)
                for label in ("worker-A", "worker-B")]
        outcomes = sorted(f.result()["outcome"] for f in futs)
    ob = outbox_for(cid, "DECISION")
    check("outcomes = one SENT + one STOPPED_DUPLICATE", outcomes == ["SENT", "STOPPED_DUPLICATE"], outcomes)
    check("exactly one outbox row", len(ob) == 1, len(ob))
    check("provider received exactly one delivery", len(deliveries_for(ob[0]["idempotency_key"])) == 1, None)
    return {"case_id": cid, "outcomes": outcomes}

FABRICATED = json.dumps({
    "known": [
        {"claim": "Review queue shows the startup has been approved", "source_table": "review_queue",
         "source_column": "status", "value": "Approved"},
        {"claim": "Tracker was updated by sync-bot", "source_table": "program_tracker",
         "source_column": "updated_by", "value": "sync-bot"}],
    "unknown": []})

def t6(check):
    r = process_inbound(msg(6, "tommy@soul.example", "Hi team, are we approved?"), ctx="TEST", ai_override=FABRICATED)
    s = rows(f"SELECT * FROM {FQ}.case_summaries WHERE case_id = :c", c=r["case_id"])[0]
    errs = json.loads(s["validation_errors"])
    check("validator rejected the AI summary", s["validation_passed"] is False, errs)
    check("fabricated value 'Approved' flagged", any("'Approved' not found" in e for e in errs), None)
    check("template version used and logged", s["version_used"] == "TEMPLATE", s["version_used"])
    summ = json.loads(s["summary_json"])
    check("template lists the expected unknowns", len(summ["unknown"]) >= 4, [u["question"] for u in summ["unknown"]])
    return {"case_id": r["case_id"], "validation_errors": errs}

def t7(check):
    spoof = "tommy@soul-labs.example"
    r = process_inbound(msg(7, spoof, "Hi team, are we approved?", subject="Soul Labs", startup_hint="STU-0427"), ctx="TEST")
    cid = r["case_id"]
    a1 = send_email(cid, "ACKNOWLEDGEMENT", "test: acknowledgement to spoofed sender", ctx="TEST")
    a2 = send_email(cid, "DECISION", "test: decision to spoofed sender", ctx="TEST")
    c = get_case(cid)
    to_spoof = pg_rows(f"SELECT count(*) n FROM {PG_SCHEMA}.provider_deliveries WHERE recipient = %s", (spoof,))[0]["n"]
    alert = outbox_for(cid, "LEAD_ALERT")
    check("case routed to the program lead (NEEDS_HUMAN)", c["case_state"] == "NEEDS_HUMAN", (c["case_state"], c["needs_human_reason"]))
    check("program lead alerted about the unverified sender", len(alert) == 1 and alert[0]["recipient"] == LEAD, None)
    check("records never gathered or summarized", not rows(f"SELECT 1 FROM {FQ}.case_summaries WHERE case_id = :c", c=cid), None)
    check("acknowledgement and decision emails both blocked", a1["outcome"] == a2["outcome"] == "BLOCKED_BY_GATE", (a1["outcome"], a2["outcome"]))
    check("nothing ever delivered to the spoofed address", to_spoof == 0, to_spoof)
    return {"case_id": cid, "reason": c["needs_human_reason"]}

def t8(check):
    cid = state["t1_case"]
    c = get_case(cid)
    reminded = check_deadlines(now=c["response_deadline"] + timedelta(hours=1), ctx="TEST")
    c2 = get_case(cid)
    a = send_email(cid, "DECISION", "test: send after deadline", ctx="TEST")
    rem = outbox_for(cid, "LEAD_REMINDER")
    check("program lead reminded once the 2-day deadline passed", cid in reminded and len(rem) == 1 and rem[0]["recipient"] == LEAD,
          c2["escalated_to"])
    check("case still waits for the lead", c2["case_state"] == "AWAITING_DECISION", c2["case_state"])
    check("decision email still blocked (never sent automatically)", a["outcome"] == "BLOCKED_BY_GATE", a["outcome"])
    return {"case_id": cid}

TESTS = [
    (1, "Conflicting records -> founder acknowledged, lead alerted, no decision email", t1),
    (2, "Lead approves -> approval email sent exactly once (repeat stopped)", t2),
    (3, "Same inbound email delivered twice -> exactly one case, one acknowledgement", t3),
    (4, "Lead rejects + provider timeout -> rejection drafted, outbox UNKNOWN, no auto-retry", t4),
    (5, "Two concurrent sends of the same decision -> one SENT, one STOPPED_DUPLICATE", t5),
    (6, "AI summary with fabricated claim -> validator rejects, template used", t6),
    (7, "Spoofed sender -> nothing sent to sender, program lead alerted", t7),
    (8, "2-business-day deadline passes -> lead reminded, nothing sent automatically", t8),
]
for no, name, fn in TESTS:
    run_test(no, name, fn)

# COMMAND ----------

spark.createDataFrame(results, "run_id STRING, test_no INT, test_name STRING, passed BOOLEAN, assertions STRING, details STRING, ran_at TIMESTAMP") \
     .write.mode("append").saveAsTable(f"{FQ}.test_results")
sync_outbox_mirror()
display(spark.sql(f"SELECT test_no, test_name, passed, assertions FROM {FQ}.test_results WHERE run_id = '{RUN_ID}' ORDER BY test_no"))
failed = [r[1] for r in results if not r[3]]
assert not failed, f"Failed tests: {failed}"
print(f"All {len(results)} tests passed (run_id {RUN_ID})")
