# Databricks notebook source
# MAGIC %md
# MAGIC # lib_workflow: shared workflow code
# MAGIC
# MAGIC Pulled in by the other notebooks with `%run ./lib_workflow`.
# MAGIC
# MAGIC **Design principle: code decides, AI explains, a human releases.**
# MAGIC * Reconciliation and the send gate are plain, deterministic Python.
# MAGIC * The LLM only writes a summary of what is known and unknown. Every cited value is checked
# MAGIC   against the source rows; any mismatch discards the AI version and uses a rule-based template.
# MAGIC * The program lead (Jimmy Woo) makes every approve/reject decision; the workflow only drafts and sends the email.
# MAGIC * Only `send_email()` can reach the (simulated) provider, and it always goes through `send_gate()`
# MAGIC   and then claims a row in the Lakebase outbox, where `idempotency_key` has a real Postgres UNIQUE constraint.
# MAGIC
# MAGIC All data is fictional.

# COMMAND ----------

import hashlib, json, re, threading, time, uuid
from datetime import datetime, timedelta

import mlflow
import pg8000.native
from databricks.sdk import WorkspaceClient

def _widget(name, default):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name) or default
    except Exception:
        return default

CATALOG = _widget("catalog", "workspace")
SCHEMA = _widget("schema", "program_ops")
LAKEBASE_ENDPOINT = _widget("lakebase_endpoint", "projects/program-ops/branches/production/endpoints/primary")
LLM_ENDPOINT = _widget("llm_endpoint", "databricks-llama-4-maverick")
FQ = f"{CATALOG}.{SCHEMA}"
PG_SCHEMA = "program_ops"

try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
except Exception:
    pass

CURRENT_USER = spark.sql("SELECT current_user() AS u").first()["u"]
MLFLOW_EXPERIMENT = f"/Users/{CURRENT_USER}/program_ops_case_summaries"
try:
    mlflow.set_experiment(MLFLOW_EXPERIMENT)
except Exception as e:  # tracing is best-effort; the workflow must not depend on it
    print("MLflow experiment not set:", e)

PROGRAM_LEAD = {"name": "Jimmy Woo", "login": "j.woo", "email": "jimmy.woo@accelerator.example", "title": "program lead"}
SYSTEM_ACTOR = ("workflow", "system")

def utcnow():
    return datetime.utcnow().replace(microsecond=0)

def ts(s):
    return datetime.fromisoformat(s) if isinstance(s, str) else s

def _jsonable(o):
    if isinstance(o, datetime):
        return o.isoformat(sep=" ")
    return str(o)

def jdump(o):
    return json.dumps(o, default=_jsonable, sort_keys=False)

# COMMAND ----------

# MAGIC %md ## Spark / Delta helpers

# COMMAND ----------

def sql(stmt, **params):
    """spark.sql with named parameters; None values are inlined as NULL (typed via CAST in the statement)."""
    args = {}
    for k, v in params.items():
        if v is None:
            stmt = re.sub(rf":{k}\b", "NULL", stmt)
        else:
            args[k] = v
    return spark.sql(stmt, args=args) if args else spark.sql(stmt)

def rows(stmt, **params):
    return [r.asDict(recursive=True) for r in sql(stmt, **params).collect()]

_seq_lock = threading.Lock()
_last_seq = [0]

def _next_seq():
    with _seq_lock:
        n = max(time.time_ns(), _last_seq[0] + 1)
        _last_seq[0] = n
        return n

def audit(case_id, startup_id, event_type, headline, details=None, actor=SYSTEM_ACTOR, now=None, ctx="STORY"):
    """Append-only event log (table property delta.appendOnly = true)."""
    sql(f"""INSERT INTO {FQ}.audit_log
            (event_id, seq, case_id, startup_id, run_context, event_time, recorded_at,
             event_type, actor, actor_type, headline, details)
            VALUES (:eid, :seq, :cid, :sid, :ctx, :et, current_timestamp(), :etype, :actor, :atype, :hl, :det)""",
        eid=str(uuid.uuid4()), seq=_next_seq(), cid=case_id, sid=startup_id, ctx=ctx,
        et=now or utcnow(), etype=event_type, actor=actor[0], atype=actor[1], hl=headline,
        det=jdump(details or {}))

def get_case(case_id):
    r = rows(f"SELECT * FROM {FQ}.cases WHERE case_id = :c", c=case_id)
    return r[0] if r else None

def update_case(case_id, now=None, **fields):
    sets, params = [], {"cid": case_id, "upd": now or utcnow()}
    for i, (k, v) in enumerate(fields.items()):
        sets.append(f"{k} = :p{i}")
        params[f"p{i}"] = v
    sql(f"UPDATE {FQ}.cases SET {', '.join(sets)}, updated_at = :upd WHERE case_id = :cid", **params)

def founder_for(startup_id):
    r = rows(f"SELECT * FROM {FQ}.founders WHERE startup_id = :s", s=startup_id)
    return r[0] if r else None

# COMMAND ----------

# MAGIC %md ## Lakebase (Postgres) connection: outbox with a real UNIQUE constraint

# COMMAND ----------

_w = WorkspaceClient()
_pg_cache = {}

class _PG:
    """Thin wrapper over pg8000.native (pure Python: the psycopg binary wheel aborts on this serverless image).
    Accepts %s placeholders; autocommit per statement."""
    def __init__(self, conn):
        self.conn = conn
    def execute(self, stmt, params=()):
        parts = stmt.split("%s")
        q = parts[0] + "".join(f":p{i}{part}" for i, part in enumerate(parts[1:]))
        res = self.conn.run(q, **{f"p{i}": v for i, v in enumerate(params)})
        return _Result(res, [c["name"] for c in (self.conn.columns or [])])
    def __enter__(self):
        return self
    def __exit__(self, *a):
        self.conn.close()

class _Result:
    def __init__(self, data, cols):
        self.data, self.cols = data or [], cols
    def fetchone(self):
        return self.data[0] if self.data else None
    def fetchall(self):
        return self.data

def pg_connect():
    """New connection per call (threads must not share one). OAuth token cached ~50 min."""
    if "host" not in _pg_cache:
        _pg_cache["host"] = _w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host
    if _pg_cache.get("token_exp", 0) < time.time():
        _pg_cache["token"] = _w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token
        _pg_cache["token_exp"] = time.time() + 50 * 60
    return _PG(pg8000.native.Connection(user=CURRENT_USER, host=_pg_cache["host"], database="databricks_postgres",
                                        password=_pg_cache["token"], ssl_context=True, timeout=30))

def pg_rows(stmt, params=()):
    with pg_connect() as conn:
        r = conn.execute(stmt, params)
        return [dict(zip(r.cols, row)) for row in r.fetchall()]

# COMMAND ----------

# MAGIC %md ## 1. Intake: dedupe by message_id, verify sender, classify intent

# COMMAND ----------

STARTUP_ID_RE = re.compile(r"\bSTU-\d{4}\b")
INTENT_LABELS = ["status_inquiry", "document_submission", "scheduling", "other"]

def case_id_for(message_id):
    return "CASE-" + hashlib.sha256(message_id.encode()).hexdigest()[:8].upper()

def verify_sender(from_email, subject, body):
    """Deterministic: sender must be exactly the founder email on file for the startup.
    Returns (verified, startup_id_or_claimed, reason)."""
    addr = from_email.strip().lower()
    by_email = rows(f"SELECT startup_id FROM {FQ}.founders WHERE lower(founder_email) = :e", e=addr)
    claimed = STARTUP_ID_RE.findall(f"{subject} {body}")
    if by_email:
        sid = by_email[0]["startup_id"]
        if claimed and claimed[0] != sid:
            return False, claimed[0], f"sender is founder of {sid} but email references {claimed[0]}"
        return True, sid, "sender matches founder email on file"
    # look for a lookalike domain to make the reviewer's job easier (evidence only, never trusted)
    domain = addr.split("@")[-1]
    near = rows(f"""SELECT startup_id, domain FROM {FQ}.founders
                    WHERE :d LIKE concat('%', split(domain, '\\\\.')[0], '%')""", d=domain)
    sid = claimed[0] if claimed else (near[0]["startup_id"] if near else None)
    return False, sid, (f"sender {addr} is not the founder email on file"
                        + (f"; domain resembles {near[0]['domain']} (possible spoof)" if near else ""))

def classify_intent(body):
    labels = json.dumps(INTENT_LABELS)
    r = sql(f"SELECT ai_classify(:b, '{labels}', map('version','2.0')) AS c", b=body).first()["c"]
    parsed = json.loads(r) if isinstance(r, str) else json.loads(r.toJson())
    resp = parsed.get("response") or ["other"]
    return resp[0], parsed

def ingest_inbound(msg, ctx="STORY", now=None):
    """Insert the inbound email only if message_id is new (MERGE = atomic insert-if-absent).
    Returns (case_id, is_new)."""
    now = now or ts(msg["received_at"])
    cid = case_id_for(msg["message_id"])
    m = sql(f"""MERGE INTO {FQ}.inbound_emails t
                USING (SELECT :mid AS message_id) s ON t.message_id = s.message_id
                WHEN MATCHED THEN UPDATE SET t.delivered_count = t.delivered_count + 1
                WHEN NOT MATCHED THEN INSERT (message_id, from_email, to_email, subject, body, received_at,
                     delivered_count, case_id, run_context)
                  VALUES (:mid, :frm, :to, :subj, :body, :rcv, 1, :cid, :ctx)""",
            mid=msg["message_id"], frm=msg["from_email"], to=msg.get("to_email", "programs@accelerator.example"),
            subj=msg["subject"], body=msg["body"], rcv=ts(msg["received_at"]), cid=cid, ctx=ctx).first().asDict()
    inserted = int(m.get("num_inserted_rows", 0))
    if not inserted:
        existing = rows(f"SELECT case_id, delivered_count FROM {FQ}.inbound_emails WHERE message_id = :m",
                        m=msg["message_id"])[0]
        audit(existing["case_id"], None, "DUPLICATE_INBOUND_IGNORED",
              f"Same message_id delivered again (delivery #{existing['delivered_count']}); no new case opened",
              {"message_id": msg["message_id"], "existing_case_id": existing["case_id"]}, now=now, ctx=ctx)
        return existing["case_id"], False
    return cid, True

# COMMAND ----------

# MAGIC %md ## 2. Gather: every record for the startup from both systems

# COMMAND ----------

def gather(startup_id):
    tracker = rows(f"SELECT * FROM {FQ}.program_tracker WHERE startup_id = :s ORDER BY updated_at", s=startup_id)
    queue = rows(f"SELECT * FROM {FQ}.review_queue WHERE startup_id = :s ORDER BY updated_at", s=startup_id)
    return {"program_tracker": tracker, "review_queue": queue}

# COMMAND ----------

# MAGIC %md ## 3. Reconcile (deterministic, no AI)

# COMMAND ----------

STATUS_MAP = {
    "approved": "APPROVED",
    "awaiting review": "NOT_APPROVED", "under review": "NOT_APPROVED", "in review": "NOT_APPROVED", "pending": "NOT_APPROVED",
    "rejected": "NOT_APPROVED", "declined": "NOT_APPROVED",
}
SIGNAL_FIELDS = {"program_tracker": ["status", "stage", "updated_at", "updated_by"],
                 "review_queue": ["status", "updated_at", "updated_by"]}

def normalize(source, rec):
    s = (rec.get("status") or "").strip().lower()
    if s == "completed":
        # "Completed" only counts as approval if the tracker says *which* stage was completed.
        return "APPROVED" if rec.get("stage") else "AMBIGUOUS"
    return STATUS_MAP.get(s, "UNRECOGNIZED")

def reconcile(records):
    evidence = []
    for source, recs in records.items():
        for r in recs:
            blanks = [f for f in SIGNAL_FIELDS[source] if r.get(f) in (None, "")]
            evidence.append({
                "source": source, "record_id": r["record_id"], "status": r.get("status"),
                "normalized": normalize(source, r), "stage": r.get("stage") if source == "program_tracker" else "n/a",
                "updated_by": r.get("updated_by"), "updated_by_type": r.get("updated_by_type"),
                "edit_kind": "human edit" if r.get("updated_by_type") == "human" else "system edit",
                "updated_at": r.get("updated_at"), "blank_fields": blanks,
            })
    n_t, n_q = len(records["program_tracker"]), len(records["review_queue"])
    if n_t == 0 or n_q == 0:
        result = "MISSING"
    else:
        norm = {e["normalized"] for e in evidence}
        result = ("AGREE_APPROVED" if norm == {"APPROVED"} else
                  "AGREE_NOT_APPROVED" if norm == {"NOT_APPROVED"} else "CONFLICT")

    newest = max(evidence, key=lambda e: e["updated_at"]) if evidence else None
    for e in evidence:
        e["is_newest"] = newest is not None and e["record_id"] == newest["record_id"]

    diff = []
    if n_t and n_q:
        t, q = records["program_tracker"][-1], records["review_queue"][-1]
        for f in ["status", "updated_at", "updated_by", "updated_by_type"]:
            if str(t.get(f)) != str(q.get(f)):
                diff.append({"field": f, "program_tracker": t.get(f), "review_queue": q.get(f)})
        diff.append({"field": "stage", "program_tracker": t.get("stage"), "review_queue": "(no such column)"})

    if result == "CONFLICT":
        if newest["updated_by_type"] == "human" and newest["normalized"] != "APPROVED":
            others = [e for e in evidence if e is not newest]
            o = others[0] if others else None
            reading = (f"Newer human edit ({newest['source']}, {newest['updated_by']}, "
                       f"{newest['updated_at']:%Y-%m-%d}) says '{newest['status']}' -> treat as NOT approved until a "
                       f"reviewer confirms." + (f" Older {o['edit_kind']} ({o['source']}, {o['updated_by']}, "
                       f"{o['updated_at']:%Y-%m-%d}) says '{o['status']}'"
                       + (" with blank " + ", ".join(o["blank_fields"]) if o["blank_fields"] else "") + "." if o else ""))
        else:
            reading = "Records disagree -> treat as NOT approved until a reviewer confirms."
    elif result == "MISSING":
        reading = "A record is missing in one system -> treat as NOT approved until a reviewer confirms."
    elif result == "AGREE_APPROVED":
        reading = "Both systems say approved."
    else:
        reading = "Both systems say not yet approved."

    return {"result": result, "evidence": evidence, "diff": diff, "suggested_reading": reading,
            "counts": {"program_tracker": n_t, "review_queue": n_q}}

# COMMAND ----------

# MAGIC %md ## 4. AI summary (explains only), validator, template fallback, MLflow tracing

# COMMAND ----------

ALLOWED_SOURCES = {
    "program_tracker": ["record_id", "startup_id", "status", "stage", "updated_at", "updated_by", "updated_by_type"],
    "review_queue": ["record_id", "startup_id", "status", "updated_at", "updated_by", "updated_by_login", "updated_by_type"],
    "founders": ["startup_id", "startup_name", "domain", "founder_name", "founder_email"],
}
NULLS = {"", "null", "none", "(blank)", "blank", "n/a"}
REQUIRED_UNKNOWNS = {  # coverage check for CONFLICT summaries (keyword match on question + why_unknown)
    "actual approval status": r"approv",
    "meaning of the ambiguous status": r"mean|ambigu|interpret",
    "supersedes / error": r"supersed|overrid|error|mistake|stale|outdated",
    "source of truth": r"source of truth|authoritative|system of record",
}
DECISION_LANGUAGE = re.compile(r"\b(is|are|has been|have been|was|were)\s+(now\s+)?(approved|rejected|accepted)\b", re.I)

def _candidates(v):
    if v is None:
        return NULLS
    if isinstance(v, datetime):
        return {v.isoformat(sep=" ").lower(), v.isoformat().lower(), v.strftime("%Y-%m-%d"),
                v.strftime("%Y-%m-%d %H:%M"), v.strftime("%Y-%m-%dT%H:%M")}
    return {str(v).strip().lower()}

def build_prompt(founder, records, recon):
    payload = {"source_tables": {"founders": [founder], **records},
               "reconciliation_by_code": {"result": recon["result"], "suggested_reading": recon["suggested_reading"],
                                          "diff": [d for d in recon["diff"] if d["field"] != "stage"]}}
    return (
        "You are assisting a human program reviewer. You do NOT decide whether the startup is approved and you do "
        "NOT write emails. Using ONLY the source rows below, return a JSON object with exactly two keys:\n"
        '{"known": [{"claim": str, "source_table": str, "source_column": str, "value": str}], '
        '"unknown": [{"question": str, "why_unknown": str}]}\n'
        "Rules for 'known': 4 to 8 items; each 'claim' is a full sentence a reviewer can read (who/what/when), citing one "
        "table under source_tables (program_tracker, review_queue or founders), one column, and the exact value from that "
        "column (write null for an empty value). Cover each record's status, who last edited it and whether that was a "
        "person or a system, when, and any blank fields. Never state or imply that the startup is approved or rejected.\n"
        "Rules for 'unknown': each item is a question a reviewer must answer, with why it cannot be answered from the "
        "data. When the records conflict, include at least: whether the startup is actually approved; what an ambiguous "
        "status means (e.g. 'Completed' with a blank stage); whether one record supersedes the other or one is an error; "
        "which system is the source of truth.\nReturn JSON only, no prose.\n\nDATA:\n" + jdump(payload))

def parse_ai_json(raw):
    if raw is None:
        raise ValueError("empty AI response")
    s = raw.strip()
    s = re.sub(r"^```(json)?", "", s).strip()
    s = re.sub(r"```$", "", s).strip()
    if not s.startswith("{"):
        m = re.search(r"\{.*\}", s, re.S)
        if not m:
            raise ValueError("no JSON object in AI response")
        s = m.group(0)
    return json.loads(s)

def validate_summary(summary, records, founder, recon):
    """Every cited value must exist in the cited table+column for this startup. Returns list of errors."""
    errors = []
    if not isinstance(summary, dict) or set(summary.keys()) != {"known", "unknown"}:
        return [f"top-level keys must be exactly known/unknown, got {list(summary.keys()) if isinstance(summary, dict) else type(summary).__name__}"]
    if not isinstance(summary["known"], list) or not isinstance(summary["unknown"], list):
        return ["known/unknown must be lists"]
    tables = dict(records, founders=[founder] if founder else [])
    for i, k in enumerate(summary["known"]):
        if not isinstance(k, dict) or not {"claim", "source_table", "source_column", "value"} <= set(k):
            errors.append(f"known[{i}] missing required fields"); continue
        t, c, v = k["source_table"], k["source_column"], k["value"]
        if t not in ALLOWED_SOURCES:
            errors.append(f"known[{i}] cites unknown table '{t}'"); continue
        if c not in ALLOWED_SOURCES[t]:
            errors.append(f"known[{i}] cites unknown column '{t}.{c}'"); continue
        claimed = NULLS if v is None else {str(v).strip().lower()}
        actual = set().union(*[_candidates(r.get(c)) for r in tables[t]]) if tables[t] else set()
        if not (claimed & actual):
            errors.append(f"known[{i}] value '{v}' not found in {t}.{c} (actual: "
                          f"{[str(r.get(c)) for r in tables[t]]})")
        if DECISION_LANGUAGE.search(str(k.get("claim", ""))):
            errors.append(f"known[{i}] claim asserts an approval decision: '{k['claim']}'")
    for i, u in enumerate(summary["unknown"]):
        if not isinstance(u, dict) or not {"question", "why_unknown"} <= set(u):
            errors.append(f"unknown[{i}] missing question/why_unknown")
    if recon["result"] in ("CONFLICT", "MISSING") and not summary["unknown"]:
        errors.append("records conflict/missing but the summary lists no unknowns")
    if recon["result"] == "CONFLICT":
        text = " ".join(f"{u.get('question', '')} {u.get('why_unknown', '')}" for u in summary["unknown"]
                        if isinstance(u, dict)).lower()
        for topic, pattern in REQUIRED_UNKNOWNS.items():
            if not re.search(pattern, text):
                errors.append(f"unknowns do not cover required topic: {topic}")
    for i, k in enumerate(summary["known"]):
        if isinstance(k, dict) and len(str(k.get("claim", "")).split()) < 4:
            errors.append(f"known[{i}] claim is not a readable sentence: '{k.get('claim')}'")
    return errors

def template_summary(founder, records, recon):
    known = []
    for source, recs in records.items():
        for r in recs:
            known.append({"claim": f"{source} record {r['record_id']} has status '{r['status']}'",
                          "source_table": source, "source_column": "status", "value": r["status"]})
            known.append({"claim": f"{source} record {r['record_id']} was last updated by {r['updated_by']} "
                                   f"({r['updated_by_type']})",
                          "source_table": source, "source_column": "updated_by", "value": r["updated_by"]})
            known.append({"claim": f"{source} record {r['record_id']} was last updated on {r['updated_at']:%Y-%m-%d}",
                          "source_table": source, "source_column": "updated_at", "value": f"{r['updated_at']:%Y-%m-%d}"})
            if source == "program_tracker":
                known.append({"claim": f"program_tracker stage is {'blank' if not r.get('stage') else repr(r['stage'])}",
                              "source_table": source, "source_column": "stage", "value": r.get("stage")})
    unknown = []
    if recon["result"] == "CONFLICT":
        unknown = [
            {"question": "Is the startup actually approved?",
             "why_unknown": "The two systems disagree and no reviewer has recorded a final decision."},
            {"question": "What does the tracker status mean?",
             "why_unknown": "The tracker's stage field is blank, so 'Completed' could mean the program, a review step, "
                            "or a sync artifact." if any(e["normalized"] == "AMBIGUOUS" for e in recon["evidence"])
                            else "The tracker status and review queue status use different vocabularies."},
            {"question": "Does one record supersede the other, or is one of them an error?",
             "why_unknown": "Neither record references the other; one is a system sync, the other a human edit."},
            {"question": "Which system is the source of truth for approval?",
             "why_unknown": "There is no documented precedence rule between program_tracker and review_queue."},
        ]
    elif recon["result"] == "MISSING":
        unknown = [{"question": "Was a record never created, or was it lost?",
                    "why_unknown": f"Record counts: {recon['counts']}."},
                   {"question": "Is the startup actually approved?",
                    "why_unknown": "Only one system has a record, so it cannot be cross-checked."}]
    return {"known": known, "unknown": unknown}

@mlflow.trace(name="case_summary", span_type="CHAIN")
def summarize_case(case_id, founder, records, recon, ai_override=None):
    """ai_override lets tests inject a model output (e.g. a fabricated claim)."""
    prompt = build_prompt(founder, records, recon)
    with mlflow.start_span(name="ai_query", span_type="LLM") as span:
        span.set_inputs({"endpoint": LLM_ENDPOINT, "prompt": prompt})
        if ai_override is not None:
            raw, err = ai_override, None
        else:
            r = sql("""SELECT ai_query(:ep, :p, failOnError => false,
                                         modelParameters => named_struct('temperature', 0.0, 'max_tokens', 1500)) AS r""",
                    ep=LLM_ENDPOINT, p=prompt).first()["r"]
            raw, err = r["result"], r["errorMessage"]
        span.set_outputs({"raw": raw, "error": err})
    with mlflow.start_span(name="validate_citations", span_type="PARSER") as span:
        try:
            parsed = parse_ai_json(raw)
            errors = validate_summary(parsed, records, founder, recon)
        except Exception as e:
            parsed, errors = None, [f"could not parse AI output: {e}"] + ([f"ai_query error: {err}"] if err else [])
        span.set_outputs({"errors": errors})
    passed = not errors
    final = parsed if passed else template_summary(founder, records, recon)
    try:
        mlflow.update_current_trace(tags={"case_id": case_id, "version_used": "AI" if passed else "TEMPLATE",
                                          "validation_passed": str(passed)})
    except Exception:
        pass
    return {"prompt": prompt, "raw": raw, "passed": passed, "errors": errors,
            "version_used": "AI" if passed else "TEMPLATE", "summary": final}

def run_summary(case_id, startup_id, founder, records, recon, now, ctx, ai_override=None):
    res = summarize_case(case_id, founder, records, recon, ai_override=ai_override)
    trace_id = None
    try:
        trace_id = mlflow.get_last_active_trace_id()
    except Exception:
        pass
    sql(f"""INSERT INTO {FQ}.case_summaries VALUES (:sid, :cid, :gen, :model, :prompt, :raw, :ok, :errs, :ver, :summ, :tr)""",
        sid=str(uuid.uuid4()), cid=case_id, gen=now, model="override (test injection)" if ai_override else LLM_ENDPOINT,
        prompt=res["prompt"], raw=res["raw"], ok=res["passed"], errs=jdump(res["errors"]),
        ver=res["version_used"], summ=jdump(res["summary"]), tr=trace_id)
    if res["passed"]:
        audit(case_id, startup_id, "AI_SUMMARY_VALIDATED",
              f"AI summary passed citation check ({len(res['summary']['known'])} known, "
              f"{len(res['summary']['unknown'])} unknown); AI version used",
              {"version_used": "AI", "model": LLM_ENDPOINT, "summary": res["summary"], "mlflow_trace_id": trace_id},
              actor=(LLM_ENDPOINT, "ai"), now=now, ctx=ctx)
    else:
        audit(case_id, startup_id, "AI_SUMMARY_REJECTED",
              f"AI summary failed validation ({len(res['errors'])} error(s)); rule-based template used instead",
              {"version_used": "TEMPLATE", "validation_errors": res["errors"], "rejected_ai_output": res["raw"],
               "summary": res["summary"], "mlflow_trace_id": trace_id}, now=now, ctx=ctx)
    return res

# COMMAND ----------

# MAGIC %md ## 5. Send gate + send-once outbox (Lakebase) + simulated provider

# COMMAND ----------

class ProviderTimeout(Exception): pass
class ProviderRejected(Exception): pass

STATUS_WORDS = re.compile(r"\b(approved|approval|rejected|accepted|completed|declined|admitted)\b", re.I)

def idempotency_key(case_id, email_type, decision_version):
    return hashlib.sha256(f"{case_id}|{email_type}|{decision_version}".encode()).hexdigest()

def business_days_after(d, n):
    while n:
        d += timedelta(days=1)
        if d.weekday() < 5:
            n -= 1
    return d.replace(hour=17, minute=0, second=0)

FOUNDER_EMAILS = ("ACKNOWLEDGEMENT", "DECISION")
LEAD_EMAILS = ("LEAD_ALERT", "LEAD_REMINDER")

def recipient_for(founder, email_type):
    """Founder emails always go to the founder on file (never the inbound From: header); alerts go to the program lead."""
    if email_type in LEAD_EMAILS:
        return PROGRAM_LEAD["email"]
    return founder["founder_email"] if founder else None

def compose(case, founder, email_type):
    if email_type == "ACKNOWLEDGEMENT":
        first = founder["founder_name"].split()[0]
        return (f"Re: your inquiry about {founder['startup_name']}",
                f"Hi {first},\n\nThank you for your email. We are now reviewing the status of your inquiry and will reply "
                f"within 2 business days (by {case['response_deadline']:%A %d %B %Y}).\n\nStartup Program Team (fictional)")
    if email_type == "DECISION":
        first = founder["founder_name"].split()[0]
        name = founder["startup_name"]
        if case["final_status"] == "APPROVED":
            return (f"{name}: you're approved for the Startup Program",
                    f"Hi {first},\n\nCongratulations! {name} has been approved for the Startup Program. "
                    f"Our team will follow up with onboarding details shortly.\n\nStartup Program Team (fictional)")
        return (f"{name}: your Startup Program application",
                f"Hi {first},\n\nThank you for applying to the Startup Program. After careful review, we are unable to offer "
                f"{name} a place in this cohort. We'd welcome a future application.\n\nStartup Program Team (fictional)")
    if email_type in LEAD_EMAILS:
        sid = case["startup_id"] or "unknown startup"
        if not case["sender_verified"]:
            return (f"[Action needed] Unverified sender asking about {sid}",
                    f"Hi {PROGRAM_LEAD['name'].split()[0]},\n\nCase {case['case_id']}: an email claiming to be about {sid} came from an "
                    f"address that does not match the founder on file. Nothing was sent to the sender.\n"
                    f"Reason: {case['needs_human_reason']}\n\nPlease check before anyone replies.")
        prefix = "[Reminder: deadline passed] " if email_type == "LEAD_REMINDER" else "[Action needed] "
        return (f"{prefix}Decide on {founder['startup_name']} ({sid})",
                f"Hi {PROGRAM_LEAD['name'].split()[0]},\n\n{founder['founder_name']} asked \"Are we approved?\" (case {case['case_id']}).\n"
                f"Records check: {case['reconciliation']}. {case['suggested_reading']}\n"
                f"The founder was told to expect an answer by {case['response_deadline']:%A %d %B}.\n\n"
                f"Please open the reviewer console, review the evidence, and choose Approve or Reject.")
    raise ValueError(email_type)

def send_gate(case, founder, email_type):
    """The ONE policy check every outbound email passes. Returns (allowed, reason, decision_version)."""
    if case is None:
        return False, "no such case", None
    if email_type in LEAD_EMAILS:
        if email_type == "LEAD_REMINDER" and case["case_state"] != "AWAITING_DECISION":
            return False, "reminders only for cases still awaiting a decision", 0
        return True, "internal alert to the program lead", 0
    if email_type not in FOUNDER_EMAILS:
        return False, f"email type {email_type} not allowed", None
    if not case["sender_verified"]:
        return False, "sender not verified as founder on file: no email of any kind goes to the sender", None
    if founder is None:
        return False, "no founder on file for startup", None
    if email_type == "ACKNOWLEDGEMENT":
        if case["response_deadline"] is None:
            return False, "acknowledgement needs a response deadline", 0
        return True, "acknowledgement (no status claim) allowed for verified founders", 0
    # DECISION
    if case["case_state"] != "RESOLVED" or case["final_status"] not in ("APPROVED", "REJECTED"):
        return False, f"decision emails require the program lead's decision (case is {case['case_state']})", case["decision_version"] or 0
    return True, f"program lead decided {case['final_status']}", case["decision_version"]

def _record_attempt(conn, key, case_id, email_type, label, now, outcome, detail):
    conn.execute(f"""INSERT INTO {PG_SCHEMA}.outbox_attempts
                     (idempotency_key, case_id, email_type, attempt_label, attempted_at, outcome, detail)
                     VALUES (%s,%s,%s,%s,%s,%s,%s)""", (key, case_id, email_type, label, now, outcome, detail))

def _provider_send(conn, key, recipient, subject, now, mode):
    """Simulated email provider. No real email is ever sent; deliveries land in provider_deliveries."""
    if mode == "reject":
        raise ProviderRejected("550 mailbox unavailable (simulated)")
    pmid = "sim-" + uuid.uuid4().hex[:12]
    conn.execute(f"""INSERT INTO {PG_SCHEMA}.provider_deliveries (provider_message_id, idempotency_key, recipient, subject, delivered_at)
                     VALUES (%s,%s,%s,%s,%s)""", (pmid, key, recipient, subject, now))
    if mode == "timeout":
        # delivered, but our client never hears back: the outcome is genuinely unknown to us
        raise ProviderTimeout("no response from provider within 10s (simulated); message may or may not have been delivered")
    return pmid

def send_email(case_id, email_type, attempt_label="workflow", now=None, provider_mode="ok", ctx="STORY",
               barrier=None):
    """The only path to the provider: gate -> claim unique outbox row -> provider -> SENT/FAILED/UNKNOWN."""
    now = now or utcnow()
    case = get_case(case_id)
    founder = founder_for(case["startup_id"]) if case and case["startup_id"] else None
    allowed, reason, version = send_gate(case, founder, email_type)
    key = idempotency_key(case_id, email_type, version if version is not None else -1)
    sid = case["startup_id"] if case else None
    recipient = recipient_for(founder, email_type)
    if not allowed:
        with pg_connect() as conn:
            _record_attempt(conn, key, case_id, email_type, attempt_label, now, "BLOCKED_BY_GATE", reason)
        audit(case_id, sid, "SEND_BLOCKED_BY_GATE", f"{email_type} blocked by send gate: {reason}",
              {"email_type": email_type, "attempt": attempt_label, "reason": reason}, now=now, ctx=ctx)
        return {"outcome": "BLOCKED_BY_GATE", "reason": reason, "idempotency_key": key}

    subject, body = compose(case, founder, email_type)
    if email_type == "ACKNOWLEDGEMENT" and STATUS_WORDS.search(body):
        reason = "acknowledgement text contains a status word"
        with pg_connect() as conn:
            _record_attempt(conn, key, case_id, email_type, attempt_label, now, "BLOCKED_BY_GATE", reason)
        audit(case_id, sid, "SEND_BLOCKED_BY_GATE", reason, {"body": body}, now=now, ctx=ctx)
        return {"outcome": "BLOCKED_BY_GATE", "reason": reason}

    with pg_connect() as conn:
        if barrier is not None:
            barrier.wait()  # test 5: release both concurrent attempts at the same instant
        row = conn.execute(f"""INSERT INTO {PG_SCHEMA}.outbox
                    (idempotency_key, case_id, email_type, decision_version, recipient, subject, body, state, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'PENDING',%s,%s)
                    ON CONFLICT (idempotency_key) DO NOTHING RETURNING outbox_id""",
                    (key, case_id, email_type, version, recipient, subject, body, now, now)).fetchone()
        if row is None:
            existing = conn.execute(f"SELECT outbox_id, state, provider_message_id, created_at FROM {PG_SCHEMA}.outbox WHERE idempotency_key=%s",
                                    (key,)).fetchone()
            detail = (f"idempotency_key already claimed by outbox_id={existing[0]} (state={existing[1]}, "
                      f"provider_message_id={existing[2]}, created {existing[3]:%Y-%m-%d %H:%M})")
            _record_attempt(conn, key, case_id, email_type, attempt_label, now, "STOPPED_DUPLICATE", detail)
            stopped = True
        else:
            stopped = False
            outbox_id = row[0]
            _record_attempt(conn, key, case_id, email_type, attempt_label, now, "CLAIMED", f"outbox_id={outbox_id}")
            try:
                pmid = _provider_send(conn, key, recipient, subject, now, provider_mode)
                conn.execute(f"UPDATE {PG_SCHEMA}.outbox SET state='SENT', provider_message_id=%s, updated_at=%s WHERE outbox_id=%s AND state='PENDING'",
                             (pmid, now, outbox_id))
                _record_attempt(conn, key, case_id, email_type, attempt_label, now, "SENT", f"provider_message_id={pmid}")
                outcome, info = "SENT", {"provider_message_id": pmid}
            except ProviderTimeout as e:
                conn.execute(f"UPDATE {PG_SCHEMA}.outbox SET state='UNKNOWN', last_error=%s, updated_at=%s WHERE outbox_id=%s",
                             (str(e), now, outbox_id))
                _record_attempt(conn, key, case_id, email_type, attempt_label, now, "UNKNOWN", str(e))
                outcome, info = "UNKNOWN", {"error": str(e)}
            except ProviderRejected as e:
                conn.execute(f"UPDATE {PG_SCHEMA}.outbox SET state='FAILED', last_error=%s, updated_at=%s WHERE outbox_id=%s",
                             (str(e), now, outbox_id))
                _record_attempt(conn, key, case_id, email_type, attempt_label, now, "FAILED", str(e))
                outcome, info = "FAILED", {"error": str(e)}

    if stopped:
        audit(case_id, sid, "SEND_STOPPED_DUPLICATE", f"{email_type} attempt '{attempt_label}' refused: already in outbox",
              {"email_type": email_type, "attempt": attempt_label, "idempotency_key": key, "detail": detail},
              now=now, ctx=ctx)
        return {"outcome": "STOPPED_DUPLICATE", "idempotency_key": key, "detail": detail}
    if outcome == "SENT":
        audit(case_id, sid, "EMAIL_SENT", f"{email_type} sent to {recipient} (simulated provider id {info['provider_message_id']})",
              {"email_type": email_type, "attempt": attempt_label, "recipient": recipient, "subject": subject, "body": body,
               "idempotency_key": key, "decision_version": version, **info}, now=now, ctx=ctx)
    elif outcome == "UNKNOWN":
        update_case(case_id, now=now, needs_human_reason="outbox UNKNOWN: provider timeout, delivery unconfirmed")
        audit(case_id, sid, "OUTBOX_UNKNOWN_ROUTED_TO_HUMAN",
              f"{email_type}: provider timed out, delivery unknown. Routed to a human; no automatic retry",
              {"email_type": email_type, "idempotency_key": key, **info}, now=now, ctx=ctx)
    else:
        audit(case_id, sid, "EMAIL_FAILED", f"{email_type} rejected by provider", {"idempotency_key": key, **info}, now=now, ctx=ctx)
    return {"outcome": outcome, "idempotency_key": key, **info}

def retry_sweeper(now=None, ctx="TEST"):
    """Periodic retry job. Never touches UNKNOWN rows: a person decides whether the email went out."""
    now = now or utcnow()
    report = []
    for r in pg_rows(f"SELECT idempotency_key, case_id, email_type, state FROM {PG_SCHEMA}.outbox WHERE state IN ('FAILED','UNKNOWN')"):
        if r["state"] == "UNKNOWN":
            audit(r["case_id"], None, "RETRY_SKIPPED_UNKNOWN",
                  "Retry sweeper skipped UNKNOWN outbox row (needs a human)", {"idempotency_key": r["idempotency_key"]},
                  now=now, ctx=ctx)
            report.append({**r, "action": "skipped (needs human)"})
        else:
            report.append({**r, "action": "eligible for retry (definite failure)"})
    return report

def sync_outbox_mirror():
    """Copy Lakebase outbox tables into Delta so the timeline view and dashboard can join them."""
    ob = pg_rows(f"SELECT * FROM {PG_SCHEMA}.outbox ORDER BY outbox_id")
    at = pg_rows(f"SELECT * FROM {PG_SCHEMA}.outbox_attempts ORDER BY attempt_id")
    dl = pg_rows(f"SELECT * FROM {PG_SCHEMA}.provider_deliveries ORDER BY delivered_at")
    for name, data, schema in [
        ("outbox_log", ob, "outbox_id BIGINT, idempotency_key STRING, case_id STRING, email_type STRING, decision_version INT, recipient STRING, subject STRING, body STRING, state STRING, provider_message_id STRING, created_at TIMESTAMP, updated_at TIMESTAMP, last_error STRING"),
        ("outbox_attempts", at, "attempt_id BIGINT, idempotency_key STRING, case_id STRING, email_type STRING, attempt_label STRING, attempted_at TIMESTAMP, outcome STRING, detail STRING"),
        ("provider_deliveries", dl, "provider_message_id STRING, idempotency_key STRING, recipient STRING, subject STRING, delivered_at TIMESTAMP"),
    ]:
        cols = [c.split()[0] for c in schema.split(", ")]
        clean = [{c: (r.get(c).replace(tzinfo=None) if isinstance(r.get(c), datetime) else r.get(c)) for c in cols} for r in data]
        spark.createDataFrame(clean, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.{name}")

# COMMAND ----------

# MAGIC %md ## 6. The workflow: verify -> acknowledge -> reconcile -> summarize -> alert the program lead

# COMMAND ----------

LEAD_ACTION = {
    "AGREE_APPROVED": "Both records say approved: confirm the approval.",
    "AGREE_NOT_APPROVED": "The application is still under review: review it and approve or reject.",
    "CONFLICT": "The records disagree: decide whether the application is approved, or review it and approve or reject.",
    "MISSING": "A record is missing in one system: review before deciding.",
}

def process_inbound(msg, ctx="STORY", now=None, ai_override=None, provider_mode="ok"):
    now = now or ts(msg["received_at"])
    later = now + timedelta(minutes=1)
    case_id, is_new = ingest_inbound(msg, ctx=ctx, now=now)
    if not is_new:
        return {"case_id": case_id, "duplicate": True}

    # Step 1: does the sender match the founder on file for this startup ID?
    verified, startup_id, why = verify_sender(msg["from_email"], msg["subject"], msg["body"])
    audit(case_id, startup_id, "INBOUND_RECEIVED", f"Email from {msg['from_email']}: \"{msg['body'][:80]}\"",
          {"message_id": msg["message_id"], "from": msg["from_email"], "subject": msg["subject"], "body": msg["body"]},
          actor=(msg["from_email"], "external"), now=now, ctx=ctx)
    intent, raw = classify_intent(msg["body"])
    sql(f"UPDATE {FQ}.inbound_emails SET intent = :i, sender_verified = :v, claimed_startup_id = :s WHERE message_id = :m",
        i=intent, v=verified, s=startup_id, m=msg["message_id"])
    audit(case_id, startup_id, "INTENT_CLASSIFIED", f"ai_classify -> {intent}", {"ai_classify": raw},
          actor=("ai_classify", "ai"), now=now, ctx=ctx)

    sql(f"""MERGE INTO {FQ}.cases t USING (SELECT :cid AS case_id) s ON t.case_id = s.case_id
            WHEN NOT MATCHED THEN INSERT (case_id, startup_id, message_id, run_context, opened_at, case_state,
                 sender_verified, hold_placed, decision_version, updated_at)
            VALUES (:cid, :sid, :mid, :ctx, :now, 'OPEN', :ver, false, 0, :now)""",
        cid=case_id, sid=startup_id, mid=msg["message_id"], ctx=ctx, now=now, ver=verified)
    audit(case_id, startup_id, "SENDER_VERIFIED" if verified else "SENDER_NOT_VERIFIED", why,
          {"from": msg["from_email"], "founder_on_file": (founder_for(startup_id) or {}).get("founder_email") if startup_id else None},
          now=now, ctx=ctx)

    if not verified or intent != "status_inquiry":
        reason = why if not verified else f"intent {intent} is not handled automatically"
        update_case(case_id, now=now, case_state="NEEDS_HUMAN", needs_human_reason=reason)
        audit(case_id, startup_id, "ROUTED_TO_PROGRAM_LEAD", f"Nothing sent to the sender; {PROGRAM_LEAD['name']} alerted ({reason})",
              {"reason": reason}, now=now, ctx=ctx)
        send_email(case_id, "LEAD_ALERT", "workflow", now=now + timedelta(seconds=30), ctx=ctx, provider_mode=provider_mode)
        return {"case_id": case_id, "state": "NEEDS_HUMAN", "reason": reason}

    # Step 2: acknowledge the founder right away (no status claim), promising an answer in 2 business days
    deadline = business_days_after(now, 2)
    update_case(case_id, now=now, response_deadline=deadline)
    send_email(case_id, "ACKNOWLEDGEMENT", "workflow", now=now + timedelta(seconds=30), ctx=ctx, provider_mode=provider_mode)

    # Step 3: reconcile every record from both systems (deterministic code), then the AI explains it
    founder = founder_for(startup_id)
    records = gather(startup_id)
    audit(case_id, startup_id, "RECORDS_GATHERED",
          f"Pulled {len(records['program_tracker'])} program_tracker + {len(records['review_queue'])} review_queue record(s)",
          {"records_side_by_side": records}, now=later, ctx=ctx)
    recon = reconcile(records)
    update_case(case_id, now=later, reconciliation=recon["result"], suggested_reading=recon["suggested_reading"],
                evidence=jdump(recon))
    audit(case_id, startup_id, "RECONCILED", f"{recon['result']}: {recon['suggested_reading']}",
          {"result": recon["result"], "diff": recon["diff"], "evidence_signals": recon["evidence"]},
          actor=("reconciler (deterministic code)", "system"), now=later, ctx=ctx)
    run_summary(case_id, startup_id, founder, records, recon, later, ctx, ai_override=ai_override)

    # Step 4: the decision email is on hold; alert the program lead to decide
    update_case(case_id, now=later, case_state="AWAITING_DECISION", hold_placed=True)
    audit(case_id, startup_id, "HOLD_PLACED",
          f"Decision email on hold until {PROGRAM_LEAD['name']} ({PROGRAM_LEAD['title']}) decides. Next step: {LEAD_ACTION[recon['result']]}",
          {"reason": recon["result"], "lead_action": LEAD_ACTION[recon["result"]], "response_deadline": deadline},
          now=later, ctx=ctx)
    send_email(case_id, "LEAD_ALERT", "workflow", now=later + timedelta(seconds=30), ctx=ctx, provider_mode=provider_mode)
    return {"case_id": case_id, "state": "AWAITING_DECISION", "deadline": deadline, "reconciliation": recon["result"]}

# COMMAND ----------

# MAGIC %md ## 7. Program lead decision, record update, draft, deadline reminder

# COMMAND ----------

def record_reviewer_decision(case_id, reviewer_name, reviewer_login, final_status, reason, now=None, ctx="STORY"):
    """The program lead approves or rejects. Both source records are updated to match, and the founder email is drafted."""
    now = now or utcnow()
    case = get_case(case_id)
    if case is None or case["case_state"] != "AWAITING_DECISION":
        raise ValueError(f"case {case_id} is not awaiting a decision (state={case and case['case_state']})")
    if final_status not in ("APPROVED", "REJECTED"):
        raise ValueError("decision must be APPROVED or REJECTED")
    if not reason.strip():
        raise ValueError("a reason is required")
    sid = case["startup_id"]
    who = (f"{reviewer_name} ({reviewer_login})", "human")
    before = gather(sid)
    version = (case["decision_version"] or 0) + 1
    new_status = "Approved" if final_status == "APPROVED" else "Rejected"

    audit(case_id, sid, "LEAD_DECISION_RECORDED", f"{reviewer_name} decided {final_status}: {reason}",
          {"decision_version": version, "final_status": final_status, "reason": reason}, actor=who, now=now, ctx=ctx)

    # both systems now say the same thing
    sql(f"""UPDATE {FQ}.program_tracker SET status = :s, stage = coalesce(stage, 'Program decision'), updated_at = :at,
            updated_by = :by, updated_by_type = 'human' WHERE startup_id = :sid""", s=new_status, at=now, by=reviewer_name, sid=sid)
    sql(f"""UPDATE {FQ}.review_queue SET status = :s, updated_at = :at, updated_by = :by, updated_by_login = :login,
            updated_by_type = 'human' WHERE startup_id = :sid""", s=new_status, at=now, by=reviewer_name, login=reviewer_login, sid=sid)
    after = gather(sid)
    changes = [f"{src}.{b['record_id']}: '{b['status']}' -> '{new_status}'" for src in before for b in before[src]]
    audit(case_id, sid, "SOURCE_RECORDS_UPDATED", "; ".join(changes),
          {"before": before, "after": after}, actor=who, now=now, ctx=ctx)

    update_case(case_id, now=now, case_state="RESOLVED", final_status=final_status, decision_version=version,
                resolved_by=f"{reviewer_name} ({reviewer_login})", hold_placed=False)
    case = get_case(case_id)
    subject, body = compose(case, founder_for(sid), "DECISION")
    sql(f"""INSERT INTO {FQ}.reviewer_decisions VALUES (:id, :cid, :v, :rn, :rl, :at, :fs, :bef, :aft, :why, :subj, :body)""",
        id=str(uuid.uuid4()), cid=case_id, v=version, rn=reviewer_name, rl=reviewer_login, at=now, fs=final_status,
        bef=jdump(before), aft=jdump(after), why=reason, subj=subject, body=body)
    audit(case_id, sid, "DECISION_EMAIL_DRAFTED",
          f"{'Approval' if final_status == 'APPROVED' else 'Rejection'} email drafted for the founder: \"{subject}\"",
          {"subject": subject, "body": body, "decision_version": version}, now=now, ctx=ctx)
    return version

def check_deadlines(now=None, ctx="STORY"):
    """Past the 2-business-day promise and still undecided: remind the program lead. Never decides or sends to the founder."""
    now = now or utcnow()
    overdue = rows(f"""SELECT case_id, startup_id, response_deadline FROM {FQ}.cases
                       WHERE case_state = 'AWAITING_DECISION' AND escalated_to IS NULL AND response_deadline < :n""", n=now)
    for c in overdue:
        update_case(c["case_id"], now=now, escalated_to=f"{PROGRAM_LEAD['name']} (reminder)")
        audit(c["case_id"], c["startup_id"], "DEADLINE_PASSED",
              f"Deadline {c['response_deadline']:%Y-%m-%d %H:%M} passed without a decision; reminding {PROGRAM_LEAD['name']}. "
              "Decision email stays on hold (never sent automatically)", {}, now=now, ctx=ctx)
        send_email(c["case_id"], "LEAD_REMINDER", "deadline check", now=now, ctx=ctx)
    return [c["case_id"] for c in overdue]
