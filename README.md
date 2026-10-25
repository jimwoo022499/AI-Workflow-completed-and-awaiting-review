# Program Ops showcase: verify → acknowledge → reconcile → program lead decides → send once

A short technical showcase built on Databricks for the *Technical Business Development Manager – Startup & VC
Ecosystem* exercise. **All data is fictional** (`.example` domains, simulated email provider; no real email is sent).

> A startup appears in two program records under the same ID. One says *Completed*, the other *Awaiting review*.
> The founder emails: *"Are we approved?"*

**Design principle: code checks the facts, AI explains them, the program lead decides.**
The AI never decides approval status and never writes or sends the decision email.

**Flow:** verify the sender → acknowledge the founder ("reviewing your inquiry, reply within 2 business days") →
reconcile both records + AI summary → alert the program lead (Jimmy Woo) → he approves or rejects → the workflow drafts the
approval/rejection email → sent exactly once (retries stopped).

## What it does

| Step | How (Databricks feature) | Rule |
|---|---|---|
| Intake | Delta `MERGE` on `message_id`, `ai_classify` | Same email never opens two cases. Sender must exactly match the founder email on file; otherwise **nothing is sent to the sender** and the program lead is alerted. |
| Acknowledge | `send_email("ACKNOWLEDGEMENT")` | Sent to every verified founder before the records are checked: "We are now reviewing the status of your inquiry and will reply within 2 business days (by <date>)". Checked for status words; deduplicated. |
| Gather | Delta queries | *Every* record for the startup ID from `program_tracker` **and** `review_queue` (no `LIMIT 1`). |
| Reconcile | Plain Python | `AGREE_APPROVED` / `AGREE_NOT_APPROVED` / `CONFLICT` / `MISSING`. Evidence signals: source, updated_by, updated_at, human vs system edit, blank fields. "Completed" with a blank stage is `AMBIGUOUS`, never approval. Produces a *suggested reading*; the case stays `UNRESOLVED`. |
| Explain | `ai_query` → JSON `{known[], unknown[]}`, traced with **MLflow** | A validator checks every cited `table.column = value` against the real rows, rejects approval language, and requires the four expected unknowns when records conflict. Any failure → AI output discarded, rule-based template used, and which version was used is logged. |
| Hold + alert | `send_gate()`, the **one** function every email passes | The decision email is blocked until the program lead decides. A `LEAD_ALERT` email goes to Jimmy Woo with the evidence and what he needs to decide. |
| Program lead decides | Notebook widgets (`04_reviewer_console`) | Jimmy sees the evidence and chooses **APPROVED** or **REJECTED** with a reason → `reviewer_decisions` + `audit_log`; both source records are updated to match; the approval or rejection email is drafted from a template. Past the 2-day deadline he gets a `LEAD_REMINDER`; nothing is ever decided or sent automatically. |
| Send once | **Lakebase Postgres** outbox, `UNIQUE (idempotency_key)` | `idempotency_key = sha256(case_id, email_type, decision_version)`. `INSERT … ON CONFLICT DO NOTHING` claims the send; the loser is logged `STOPPED_DUPLICATE`. States `PENDING → SENT / FAILED / UNKNOWN`. A timeout → `UNKNOWN` → human; the retry sweeper never touches `UNKNOWN`. A provider message ID is stored. |
| Evidence | `case_timeline` view + AI/BI dashboard | Per case, in time order: sender check, acknowledgement, records side by side, diff, AI summary + validation, lead alert, decision, drafted email, outbox log including the `STOPPED_DUPLICATE` retry. |

### Data model (`workspace.program_ops`)
Delta: `founders`, `program_tracker`, `review_queue`, `inbound_emails`, `cases`, `case_summaries`,
`reviewer_decisions`, `audit_log` (`delta.appendOnly = true`), `test_results`, plus mirrors of the Lakebase tables
(`outbox_log`, `outbox_attempts`, `provider_deliveries`) for the timeline and dashboard.
Views: `case_timeline`, `case_records_side_by_side`.
Lakebase (`program-ops` project, schema `program_ops`): `outbox` (UNIQUE idempotency_key, CHECK on state),
`outbox_attempts`, `provider_deliveries` (simulated provider's mailbox).

## Tests (job task `tests`, results in `test_results`)
1. Conflicting records → founder acknowledged once, program lead alerted, decision email blocked.
2. Program lead approves → approval email drafted and SENT exactly once (repeat → STOPPED_DUPLICATE). Records agreed, but nothing was sent before the lead decided.
3. Same inbound email delivered twice → exactly one case, one acknowledgement.
4. Program lead rejects + provider timeout → rejection email drafted, outbox UNKNOWN, retry sweeper skips it, provider called once.
5. Two **concurrent** sends of the same decision (two threads, two DB connections) → one SENT, one STOPPED_DUPLICATE.
6. AI summary with a fabricated claim → validator rejects, template used.
7. Spoofed sender (`tommy@soul-labs.example`) → nothing sent to the sender, program lead alerted, nothing gathered.
8. 2-business-day deadline passes → program lead reminded, decision email still blocked.

Then the story for STU-0427 runs on a simulated clock (Sat 2026-10-24, UTC): 09:02 Tommy's email → sender verified →
acknowledgement sent → records CONFLICT → AI summary validated → Jimmy Woo alerted (09:03) → 10:05 Jimmy approves, both records
updated, approval email drafted → 10:06 approval email SENT → 10:13 simulated retry → STOPPED_DUPLICATE.

## How to run

```bash
# prerequisites: Databricks CLI >= 0.294, auth via DATABRICKS_HOST / DATABRICKS_TOKEN (or a profile)
databricks postgres create-project program-ops --json '{"spec": {"display_name": "program-ops outbox"}}'  # once
databricks bundle validate --strict
databricks bundle deploy
databricks bundle run program_ops_showcase     # setup → tests → story_intake → reviewer_decision → story_send
```
To act as the reviewer yourself: run the job up to `story_intake` (or run `03_story_intake` by hand), open
`src/04_reviewer_console` in the workspace, read the evidence, choose APPROVED or REJECTED with a reason and `action = submit`, then run
`05_story_send`. In the automated job, the `reviewer_decision` task stands in for Jimmy Woo by passing the same widget
values; the code path is the same.

Layout:
```
databricks.yml                       bundle (variables: catalog, schema, lakebase_endpoint, llm_endpoint, warehouse_id)
resources/program_ops.job.yml        serverless job, 5 notebook tasks
resources/program_ops.dashboard.yml  AI/BI dashboard
src/lib_workflow.py                  all workflow logic (gate, reconcile, validator, outbox)
src/01_setup_and_seed.py … 05_story_send.py
src/program_ops_evidence.lvdash.json dashboard (generated by dashboard/build_dashboard.py)
results/                             exported test results, STU-0427 timeline, AI summary
```

## Design decisions
- **Uniqueness lives in the outbox store, not the code path.** The Lakebase `UNIQUE` constraint makes "send once"
  true under retries, re-runs and real concurrency (test 5). The provider is called only by the request that claimed
  the row.
- **Unknown is not failure.** A timeout may have delivered the email, so `UNKNOWN` is never retried automatically;
  a person checks the provider and decides. Only a definite `FAILED` would be eligible for retry.
- **The recipient always comes from `founders`**, never from the inbound `From:` header, and unverified senders get
  nothing (not even the acknowledgement), so a lookalike domain learns nothing.
- **The program lead decides every case**, even when the records agree: approval is a business commitment. The workflow
  does the legwork (verify, acknowledge, reconcile, summarise, draft), and the founder gets a fast acknowledgement meanwhile.
- **The AI output is untrusted input.** It is accepted only if every citation matches the source rows, it contains
  no approval language, and it covers the required unknowns. Otherwise the deterministic template is used. Both
  versions are stored in `case_summaries` with the validation errors and the MLflow trace ID (experiment
  `/Users/<you>/program_ops_case_summaries`).
- **The validator earns its keep on real output, not only on the injected test.** In the final run, 6 of 7 live
  model summaries passed; one (a happy-path test case) cited a column that does not exist (`review_queue.stage`),
  was rejected, and the template was used. The story case's AI summary passed (13 cited facts, 4 unknowns).
- **Append-only audit log** (`delta.appendOnly`) records every step with actor and actor type (human / system / ai /
  external), so the timeline can be rebuilt from it.

## Free Edition limitations and substitutions
- **No Claude model was available** (no `databricks-claude-*` endpoint or `system.ai` Claude service in this
  workspace). The AI summary uses **`databricks-llama-4-maverick`** via `ai_query`. Swapping in a Claude endpoint is a
  single bundle variable (`llm_endpoint`); the validator and template fallback stay the same.
- **Lakebase *was* available**, so the outbox has a real Postgres `UNIQUE` constraint. For reference: had it not been,
  the fallback would be Delta + `MERGE`, but **Delta PRIMARY KEY/UNIQUE constraints are informational only** (not
  enforced), and two concurrent MERGEs can both insert. The PKs declared on `inbound_emails` and `cases` are therefore
  documentation; their real protection is a deterministic `case_id = hash(message_id)` plus `MERGE`. In production you
  would harden this with Lakebase/OLTP uniqueness, or a single-writer queue partitioned by key.
- The `psycopg[binary]` wheel aborts (SIGABRT) on the serverless image, so the pure-Python `pg8000` driver is used.
- Dashboard queries use fully qualified names (`workspace.program_ops.*`) because bundle dashboards do not take a
  dataset catalog/schema.
- The Lakebase tables are mirrored into Delta at the end of each notebook for the view and dashboard. In
  production, use Lakehouse Sync (CDC) or a federated query instead.

## What I'd do in production
- **Shadow mode first:** run reconcile + gate + AI summary on live inbound mail but send nothing. Compare the
  workflow's would-send / would-hold decisions with what the human team actually did; review every disagreement.
- **Staged rollout:** acknowledgements and lead alerts first (they carry no status claim) → decision emails once the program
  lead trusts the drafts → wider. Keep a kill switch on the gate.
- **Metrics with hard targets:** **zero duplicate deliveries** (`count(*) - count(distinct idempotency_key)` in
  provider deliveries), **zero early sends** (decision emails created before the program lead decided), zero emails to
  unverified senders; plus time-to-resolution, % of cases escalated, `UNKNOWN` outbox age, AI-summary rejection rate
  (template fallbacks), and founder follow-up rate.
- **Hardening:** a real provider with its own idempotency header that carries our key; a provider webhook to settle
  `UNKNOWN`; SPF/DKIM/DMARC checks in sender verification; a Databricks App reviewer UI with SSO identity instead of
  a free-text name; Unity Catalog grants so only the workflow service principal can write the outbox; an MLflow
  evaluation set of reconciliation cases (`mlflow.genai.evaluate` with citation-groundedness scorers) run before any
  prompt or model change; alerts on `UNKNOWN` rows and escalations.
