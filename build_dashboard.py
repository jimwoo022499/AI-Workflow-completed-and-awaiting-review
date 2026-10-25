"""Generates src/program_ops_evidence.lvdash.json (AI/BI dashboard over the case evidence tables)."""
import json, pathlib

S = "workspace.program_ops"  # bundles don't support dataset_catalog/schema for dashboards, so names are qualified
DATASETS = {
    "ds_kpis": f"""
WITH latest AS (SELECT max_by(run_id, ran_at) AS run_id FROM {S}.test_results),
resolved AS (SELECT case_id, min(event_time) AS resolved_at FROM {S}.audit_log WHERE event_type = 'CASE_RESOLVED' GROUP BY case_id)
SELECT
  (SELECT count_if(passed) FROM {S}.test_results WHERE run_id = (SELECT run_id FROM latest)) AS tests_passed,
  (SELECT count(*) FROM {S}.test_results WHERE run_id = (SELECT run_id FROM latest)) AS tests_total,
  (SELECT count(*) - count(DISTINCT idempotency_key) FROM {S}.provider_deliveries) AS duplicate_deliveries,
  (SELECT count(*) FROM {S}.outbox_log o LEFT JOIN resolved r USING (case_id)
     WHERE o.email_type = 'DECISION' AND (r.resolved_at IS NULL OR o.created_at < r.resolved_at)) AS early_status_sends,
  (SELECT count(*) FROM {S}.outbox_attempts WHERE outcome = 'STOPPED_DUPLICATE') AS stopped_duplicates,
  (SELECT count(*) FROM {S}.outbox_attempts WHERE outcome = 'BLOCKED_BY_GATE') AS blocked_by_gate
""",
    "ds_timeline": f"""
SELECT step, date_format(event_time, 'yyyy-MM-dd HH:mm') AS event_time, event_type, actor, actor_type, headline, details
FROM {S}.case_timeline WHERE startup_id = 'STU-0427' AND run_context = 'STORY' ORDER BY step
""",
    "ds_tests": f"""
SELECT test_no, test_name, CASE WHEN passed THEN 'PASS' ELSE 'FAIL' END AS result, assertions, ran_at
FROM {S}.test_results WHERE run_id = (SELECT max_by(run_id, ran_at) FROM {S}.test_results) ORDER BY test_no
""",
    "ds_attempts": f"""
SELECT a.attempt_id, date_format(a.attempted_at, 'yyyy-MM-dd HH:mm') AS attempted_at, c.run_context, c.startup_id, a.case_id,
       a.email_type, a.attempt_label, a.outcome, a.detail, o.state AS outbox_state, o.provider_message_id
FROM {S}.outbox_attempts a LEFT JOIN {S}.cases c USING (case_id)
LEFT JOIN {S}.outbox_log o ON o.idempotency_key = a.idempotency_key
ORDER BY a.attempt_id
""",
    "ds_records": f"""
SELECT startup_id, startup_name, tracker_status, coalesce(tracker_stage, '(blank)') AS tracker_stage,
       date_format(tracker_updated_at, 'yyyy-MM-dd HH:mm') AS tracker_updated_at, tracker_updated_by, tracker_edit_kind,
       queue_status, date_format(queue_updated_at, 'yyyy-MM-dd HH:mm') AS queue_updated_at, queue_updated_by, queue_edit_kind
FROM {S}.case_records_side_by_side ORDER BY startup_id
""",
    "ds_cases": f"""
SELECT case_id, run_context, startup_id, case_state, reconciliation, final_status, resolved_by, escalated_to,
       needs_human_reason, suggested_reading
FROM {S}.cases ORDER BY run_context, opened_at
""",
}

def lines(q):
    return [l + "\n" for l in q.strip().splitlines()]

def text(name, md, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": md}}, "position": {"x": x, "y": y, "width": w, "height": h}}

def counter(name, field, title, x, y):
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": "ds_kpis",
                     "fields": [{"name": field, "expression": f"`{field}`"}], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "counter",
                 "encodings": {"value": {"fieldName": field, "displayName": title}},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": 2, "height": 3}}

def table(name, ds, cols, title, x, y, w, h):
    return {"widget": {"name": name,
        "queries": [{"name": "main_query", "query": {"datasetName": ds,
                     "fields": [{"name": c, "expression": f"`{c}`"} for c, _ in cols], "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table",
                 "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in cols]},
                 "frame": {"showTitle": True, "title": title}}},
        "position": {"x": x, "y": y, "width": w, "height": h}}

bar = {"widget": {"name": "attempts-by-outcome",
    "queries": [{"name": "main_query", "query": {"datasetName": "ds_attempts", "fields": [
        {"name": "outcome", "expression": "`outcome`"},
        {"name": "count(attempt_id)", "expression": "COUNT(`attempt_id`)"},
        {"name": "email_type", "expression": "`email_type`"}], "disaggregated": False}}],
    "spec": {"version": 3, "widgetType": "bar",
             "encodings": {"x": {"fieldName": "outcome", "scale": {"type": "categorical"}, "displayName": "Outcome"},
                           "y": {"fieldName": "count(attempt_id)", "scale": {"type": "quantitative"}, "displayName": "Attempts"},
                           "color": {"fieldName": "email_type", "scale": {"type": "categorical"}, "displayName": "Email type"}},
             "frame": {"showTitle": True, "title": "Every send attempt, by outcome (tests + story)"}}},
    "position": {"x": 0, "y": 22, "width": 6, "height": 6}}

layout = [
    text("title", ["# Program Ops: reconcile, hold, send once\n"], 0, 0, 12, 1),
    text("subtitle", ["Fictional data: Soul Labs (STU-0427) has conflicting records. The founder is verified and acknowledged, the records are reconciled, "
                      "and program lead Jimmy Woo decides; the drafted decision email is then sent exactly once.\n"], 0, 1, 12, 1),
    counter("kpi-tests", "tests_passed", "Tests passed", 0, 2),
    counter("kpi-total", "tests_total", "Tests run", 2, 2),
    counter("kpi-dupes", "duplicate_deliveries", "Duplicate deliveries", 4, 2),
    counter("kpi-early", "early_status_sends", "Decision emails before the lead decided", 6, 2),
    counter("kpi-stopped", "stopped_duplicates", "Retries stopped (duplicate)", 8, 2),
    counter("kpi-blocked", "blocked_by_gate", "Sends blocked by gate", 10, 2),
    table("timeline", "ds_timeline", [("step", "#"), ("event_time", "Time (UTC)"), ("event_type", "Event"),
          ("actor", "Actor"), ("actor_type", "Actor type"), ("headline", "What happened"), ("details", "Evidence (JSON)")],
          "STU-0427 case timeline (reviewer evidence)", 0, 5, 12, 9),
    table("records", "ds_records", [("startup_id", "Startup"), ("startup_name", "Name"), ("tracker_status", "Tracker status"),
          ("tracker_stage", "Tracker stage"), ("tracker_updated_at", "Tracker updated"), ("tracker_updated_by", "by"),
          ("tracker_edit_kind", "edit"), ("queue_status", "Queue status"), ("queue_updated_at", "Queue updated"),
          ("queue_updated_by", "by"), ("queue_edit_kind", "edit")], "Source records side by side (current)", 0, 14, 12, 4),
    table("tests", "ds_tests", [("test_no", "#"), ("test_name", "Test"), ("result", "Result"), ("assertions", "Assertions")],
          "Test results (latest run)", 0, 18, 12, 4),
    bar,
    table("cases", "ds_cases", [("case_id", "Case"), ("run_context", "Context"), ("startup_id", "Startup"),
          ("case_state", "State"), ("reconciliation", "Reconciliation"), ("final_status", "Final"),
          ("escalated_to", "Escalated to"), ("needs_human_reason", "Needs human")], "All cases", 6, 22, 6, 6),
    table("attempts", "ds_attempts", [("attempt_id", "#"), ("attempted_at", "Time"), ("run_context", "Context"),
          ("startup_id", "Startup"), ("email_type", "Email"), ("attempt_label", "Attempt"), ("outcome", "Outcome"),
          ("outbox_state", "Outbox state"), ("provider_message_id", "Provider msg id"), ("detail", "Detail")],
          "Outbox attempt log (Lakebase, mirrored)", 0, 28, 12, 7),
]

dash = {
    "datasets": [{"name": k, "displayName": k.replace("ds_", ""), "queryLines": lines(v)} for k, v in DATASETS.items()],
    "pages": [{"name": "evidence", "displayName": "Evidence", "pageType": "PAGE_TYPE_CANVAS",
               "layoutVersion": "GRID_V1", "layout": layout}],
    "uiSettings": {"theme": {
        "canvasBackgroundColor": {"light": "#FCFCFC", "dark": "#1F272D"},
        "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
        "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
        "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
        "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
        "visualizationColors": ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#D55E00", "#56B4E9"],
        "widgetHeaderAlignment": "LEFT"}},
}
out = pathlib.Path(__file__).resolve().parent.parent / "src" / "program_ops_evidence.lvdash.json"
out.write_text(json.dumps(dash, indent=2))
print("wrote", out)
