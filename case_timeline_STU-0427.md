# case_timeline: STU-0427 (Soul Labs), story run

Simulated clock, UTC. `source` = audit_log (Delta) or lakebase.outbox_attempts (Postgres, mirrored to Delta).

| Step | Time | Event | Actor | What happened |
|---|---|---|---|---|
| 1 | 09:02 | INBOUND_RECEIVED | tommy@soul.example (external) | Email from tommy@soul.example: "Hi team, are we approved?" |
| 2 | 09:02 | INTENT_CLASSIFIED | ai_classify (ai) | ai_classify -> status_inquiry |
| 3 | 09:02 | SENDER_VERIFIED | workflow (system) | sender matches founder email on file |
| 4 | 09:02 | OUTBOX_CLAIMED | workflow (system) | ACKNOWLEDGEMENT attempt "workflow": CLAIMED (outbox_id=22) |
| 5 | 09:02 | OUTBOX_SENT | workflow (system) | ACKNOWLEDGEMENT attempt "workflow": SENT (provider_message_id=sim-16487accb69c) |
| 6 | 09:02 | EMAIL_SENT | workflow (system) | ACKNOWLEDGEMENT sent to tommy@soul.example (simulated provider id sim-16487accb69c) |
| 7 | 09:03 | RECORDS_GATHERED | workflow (system) | Pulled 1 program_tracker + 1 review_queue record(s) |
| 8 | 09:03 | RECONCILED | reconciler (deterministic code) (system) | CONFLICT: Newer human edit (review_queue, Jimmy Woo, 2026-10-19) says 'Awaiting review' -> treat as NOT approved until a reviewer confirms. Older system edit (program_tracker, sync-bot, 2026-10-14) says 'Completed' with blank stage. |
| 9 | 09:03 | AI_SUMMARY_VALIDATED | databricks-llama-4-maverick (ai) | AI summary passed citation check (8 known, 4 unknown); AI version used |
| 10 | 09:03 | HOLD_PLACED | workflow (system) | Decision email on hold until Jimmy Woo (program lead) decides. Next step: The records disagree: decide whether the application is approved, or review it and approve or reject. |
| 11 | 09:03 | OUTBOX_CLAIMED | workflow (system) | LEAD_ALERT attempt "workflow": CLAIMED (outbox_id=23) |
| 12 | 09:03 | OUTBOX_SENT | workflow (system) | LEAD_ALERT attempt "workflow": SENT (provider_message_id=sim-6bbc10dd7801) |
| 13 | 09:03 | EMAIL_SENT | workflow (system) | LEAD_ALERT sent to jimmy.woo@accelerator.example (simulated provider id sim-6bbc10dd7801) |
| 14 | 10:05 | LEAD_DECISION_RECORDED | Jimmy Woo (j.woo) (human) | Jimmy Woo decided APPROVED: Reviewed 2026-10-24: diligence complete and program completion confirmed. Tracker 'Completed' stands; the queue entry was stale. Approve. |
| 15 | 10:05 | SOURCE_RECORDS_UPDATED | Jimmy Woo (j.woo) (human) | program_tracker.PT-0427-01: 'Completed' -> 'Approved'; review_queue.RQ-0427-01: 'Awaiting review' -> 'Approved' |
| 16 | 10:05 | DECISION_EMAIL_DRAFTED | workflow (system) | Approval email drafted for the founder: "Soul Labs: you're approved for the Startup Program" |
| 17 | 10:06 | OUTBOX_CLAIMED | workflow: send after decision (system) | DECISION attempt "workflow: send after decision": CLAIMED (outbox_id=24) |
| 18 | 10:06 | OUTBOX_SENT | workflow: send after decision (system) | DECISION attempt "workflow: send after decision": SENT (provider_message_id=sim-450077f652c3) |
| 19 | 10:06 | EMAIL_SENT | workflow (system) | DECISION sent to tommy@soul.example (simulated provider id sim-450077f652c3) |
| 20 | 10:13 | OUTBOX_STOPPED_DUPLICATE | simulated retry at 10:13 (system) | DECISION attempt "simulated retry at 10:13": STOPPED_DUPLICATE (idempotency_key already claimed by outbox_id=24 (state=SENT, provider_message_id=sim-450077f652c3, created 2026-10-24 10:06)) |
| 21 | 10:13 | SEND_STOPPED_DUPLICATE | workflow (system) | DECISION attempt 'simulated retry at 10:13' refused: already in outbox |

## Evidence details (JSON per step)

### 1. INBOUND_RECEIVED
```json
{
  "message_id": "<20261024090200.soul0427@soul.example>",
  "from": "tommy@soul.example",
  "subject": "Quick question",
  "body": "Hi team, are we approved?"
}
```
### 2. INTENT_CLASSIFIED
```json
{
  "ai_classify": {
    "error_message": null,
    "metadata": {
      "version": "2.0"
    },
    "response": [
      "status_inquiry"
    ]
  }
}
```
### 3. SENDER_VERIFIED
```json
{
  "from": "tommy@soul.example",
  "founder_on_file": "tommy@soul.example"
}
```
### 4. OUTBOX_CLAIMED
```json
{
  "idempotency_key": "c44c8b9ce4778b7b4246c1f05129d23549c46a968807371d8621cf2d85e33904",
  "outbox_state_now": "SENT",
  "provider_message_id": "sim-16487accb69c"
}
```
### 5. OUTBOX_SENT
```json
{
  "idempotency_key": "c44c8b9ce4778b7b4246c1f05129d23549c46a968807371d8621cf2d85e33904",
  "outbox_state_now": "SENT",
  "provider_message_id": "sim-16487accb69c"
}
```
### 6. EMAIL_SENT
```json
{
  "email_type": "ACKNOWLEDGEMENT",
  "attempt": "workflow",
  "recipient": "tommy@soul.example",
  "subject": "Re: your inquiry about Soul Labs",
  "body": "Hi Tommy,\n\nThank you for your email. We are now reviewing the status of your inquiry and will reply within 2 business days (by Tuesday 27 October 2026).\n\nStartup Program Team (fictional)",
  "idempotency_key": "c44c8b9ce4778b7b4246c1f05129d23549c46a968807371d8621cf2d85e33904",
  "decision_version": 0,
  "provider_message_id": "sim-16487accb69c"
}
```
### 7. RECORDS_GATHERED
```json
{
  "records_side_by_side": {
    "program_tracker": [
      {
        "record_id": "PT-0427-01",
        "startup_id": "STU-0427",
        "status": "Completed",
        "stage": null,
        "updated_at": "2026-10-14 08:00:00",
        "updated_by": "sync-bot",
        "updated_by_type": "system"
      }
    ],
    "review_queue": [
      {
        "record_id": "RQ-0427-01",
        "startup_id": "STU-0427",
        "status": "Awaiting review",
        "updated_at": "2026-10-19 14:30:00",
        "updated_by": "Jimmy Woo",
        "updated_by_login": "j.woo",
        "updated_by_type": "human"
      }
    ]
  }
}
```
### 8. RECONCILED
```json
{
  "result": "CONFLICT",
  "diff": [
    {
      "field": "status",
      "program_tracker": "Completed",
      "review_queue": "Awaiting review"
    },
    {
      "field": "updated_at",
      "program_tracker": "2026-10-14 08:00:00",
      "review_queue": "2026-10-19 14:30:00"
    },
    {
      "field": "updated_by",
      "program_tracker": "sync-bot",
      "review_queue": "Jimmy Woo"
    },
    {
      "field": "updated_by_type",
      "program_tracker": "system",
      "review_queue": "human"
    },
    {
      "field": "stage",
      "program_tracker": null,
      "review_queue": "(no such column)"
    }
  ],
  "evidence_signals": [
    {
      "source": "program_tracker",
      "record_id": "PT-0427-01",
      "status": "Completed",
      "normalized": "AMBIGUOUS",
      "stage": null,
      "updated_by": "sync-bot",
      "updated_by_type": "system",
      "edit_kind": "system edit",
      "updated_at": "2026-10-14 08:00:00",
      "blank_fields": [
        "stage"
      ],
      "is_newest": false
    },
    {
      "source": "review_queue",
      "record_id": "RQ-0427-01",
      "status": "Awaiting review",
      "normalized": "NOT_APPROVED",
      "stage": "n/a",
      "updated_by": "Jimmy Woo",
      "updated_by_type": "human",
      "edit_kind": "human edit",
      "updated_at": "2026-10-19 14:30:00",
      "blank_fields": [],
      "is_newest": true
    }
  ]
}
```
### 9. AI_SUMMARY_VALIDATED
```json
{
  "version_used": "AI",
  "model": "databricks-llama-4-maverick",
  "summary": {
    "known": [
      {
        "claim": "The startup Soul Labs with ID STU-0427 has a founder named Tommy Lam.",
        "source_table": "founders",
        "source_column": "founder_name",
        "value": "Tommy Lam"
      },
      {
        "claim": "The record PT-0427-01 for STU-0427 in program_tracker has a status of Completed.",
        "source_table": "program_tracker",
        "source_column": "status",
        "value": "Completed"
      },
      {
        "claim": "The stage for record PT-0427-01 in program_tracker is blank.",
        "source_table": "program_tracker",
        "source_column": "stage",
        "value": "null"
      },
      {
        "claim": "The record PT-0427-01 was last updated at 2026-10-14 08:00:00.",
        "source_table": "program_tracker",
        "source_column": "updated_at",
        "value": "2026-10-14 08:00:00"
      },
      {
        "claim": "The record PT-0427-01 was last updated by sync-bot, which is a system.",
        "source_table": "program_tracker",
        "source_column": "updated_by_type",
        "value": "system"
      },
      {
        "claim": "The record RQ-0427-01 for STU-0427 in review_queue has a status of Awaiting review.",
        "source_table": "review_queue",
        "source_column": "status",
        "value": "Awaiting review"
      },
      {
        "claim": "The record RQ-0427-01 was last updated at 2026-10-19 14:30:00.",
        "source_table": "review_queue",
        "source_column": "updated_at",
        "value": "2026-10-19 14:30:00"
      },
      {
        "claim": "The record RQ-0427-01 was last updated by Jimmy Woo, who is a human.",
        "source_table": "review_queue",
        "source_column": "updated_by_type",
        "value": "human"
      }
    ],
    "unknown": [
      {
        "question": "Is the startup STU-0427 actually approved?",
        "why_unknown": "The records in program_tracker and review_queue conflict regarding the status."
      },
      {
        "question": "What does the status 'Completed' with a blank stage in program_tracker mean?",
        "why_unknown": "The stage is blank, making the status ambiguous."
      },
      {
        "question": "Which record supersedes the other, or is one an error?",
        "why_unknown": "The reconciliation result is CONFLICT, indicating conflicting information between program_tracker and review_queue."
      },
      {
        "question": "Which system is the source of truth for the startup's status?",
        "why_unknown": "The reconciliation result suggests considering the newer human edit in review_queue, but this needs confirmation."
      }
    ]
  },
  "mlflow_trace_id": "tr-321e577c5adfa97d0bd91086127535ac"
}
```
### 10. HOLD_PLACED
```json
{
  "reason": "CONFLICT",
  "lead_action": "The records disagree: decide whether the application is approved, or review it and approve or reject.",
  "response_deadline": "2026-10-27 17:00:00"
}
```
### 11. OUTBOX_CLAIMED
```json
{
  "idempotency_key": "6a6ed94f220bdca8004f60b66fbfaab11c994c3d66f3018c39876cb36b260054",
  "outbox_state_now": "SENT",
  "provider_message_id": "sim-6bbc10dd7801"
}
```
### 12. OUTBOX_SENT
```json
{
  "idempotency_key": "6a6ed94f220bdca8004f60b66fbfaab11c994c3d66f3018c39876cb36b260054",
  "outbox_state_now": "SENT",
  "provider_message_id": "sim-6bbc10dd7801"
}
```
### 13. EMAIL_SENT
```json
{
  "email_type": "LEAD_ALERT",
  "attempt": "workflow",
  "recipient": "jimmy.woo@accelerator.example",
  "subject": "[Action needed] Decide on Soul Labs (STU-0427)",
  "body": "Hi Jimmy,\n\nTommy Lam asked \"Are we approved?\" (case CASE-5F7DD0CB).\nRecords check: CONFLICT. Newer human edit (review_queue, Jimmy Woo, 2026-10-19) says 'Awaiting review' -> treat as NOT approved until a reviewer confirms. Older system edit (program_tracker, sync-bot, 2026-10-14) says 'Completed' with blank stage.\nThe founder was told to expect an answer by Tuesday 27 October.\n\nPlease open the reviewer console, review the evidence, and choose Approve or Reject.",
  "idempotency_key": "6a6ed94f220bdca8004f60b66fbfaab11c994c3d66f3018c39876cb36b260054",
  "decision_version": 0,
  "provider_message_id": "sim-6bbc10dd7801"
}
```
### 14. LEAD_DECISION_RECORDED
```json
{
  "decision_version": 1,
  "final_status": "APPROVED",
  "reason": "Reviewed 2026-10-24: diligence complete and program completion confirmed. Tracker 'Completed' stands; the queue entry was stale. Approve."
}
```
### 15. SOURCE_RECORDS_UPDATED
```json
{
  "before": {
    "program_tracker": [
      {
        "record_id": "PT-0427-01",
        "startup_id": "STU-0427",
        "status": "Completed",
        "stage": null,
        "updated_at": "2026-10-14 08:00:00",
        "updated_by": "sync-bot",
        "updated_by_type": "system"
      }
    ],
    "review_queue": [
      {
        "record_id": "RQ-0427-01",
        "startup_id": "STU-0427",
        "status": "Awaiting review",
        "updated_at": "2026-10-19 14:30:00",
        "updated_by": "Jimmy Woo",
        "updated_by_login": "j.woo",
        "updated_by_type": "human"
      }
    ]
  },
  "after": {
    "program_tracker": [
      {
        "record_id": "PT-0427-01",
        "startup_id": "STU-0427",
        "status": "Approved",
        "stage": "Program decision",
        "updated_at": "2026-10-24 10:05:00",
        "updated_by": "Jimmy Woo",
        "updated_by_type": "human"
      }
    ],
    "review_queue": [
      {
        "record_id": "RQ-0427-01",
        "startup_id": "STU-0427",
        "status": "Approved",
        "updated_at": "2026-10-24 10:05:00",
        "updated_by": "Jimmy Woo",
        "updated_by_login": "j.woo",
        "updated_by_type": "human"
      }
    ]
  }
}
```
### 16. DECISION_EMAIL_DRAFTED
```json
{
  "subject": "Soul Labs: you're approved for the Startup Program",
  "body": "Hi Tommy,\n\nCongratulations! Soul Labs has been approved for the Startup Program. Our team will follow up with onboarding details shortly.\n\nStartup Program Team (fictional)",
  "decision_version": 1
}
```
### 17. OUTBOX_CLAIMED
```json
{
  "idempotency_key": "bec121f99aa93ebfaa7a27b668e9203729e8cd666d7949c7cbfd8a49e0e9a21b",
  "outbox_state_now": "SENT",
  "provider_message_id": "sim-450077f652c3"
}
```
### 18. OUTBOX_SENT
```json
{
  "idempotency_key": "bec121f99aa93ebfaa7a27b668e9203729e8cd666d7949c7cbfd8a49e0e9a21b",
  "outbox_state_now": "SENT",
  "provider_message_id": "sim-450077f652c3"
}
```
### 19. EMAIL_SENT
```json
{
  "email_type": "DECISION",
  "attempt": "workflow: send after decision",
  "recipient": "tommy@soul.example",
  "subject": "Soul Labs: you're approved for the Startup Program",
  "body": "Hi Tommy,\n\nCongratulations! Soul Labs has been approved for the Startup Program. Our team will follow up with onboarding details shortly.\n\nStartup Program Team (fictional)",
  "idempotency_key": "bec121f99aa93ebfaa7a27b668e9203729e8cd666d7949c7cbfd8a49e0e9a21b",
  "decision_version": 1,
  "provider_message_id": "sim-450077f652c3"
}
```
### 20. OUTBOX_STOPPED_DUPLICATE
```json
{
  "idempotency_key": "bec121f99aa93ebfaa7a27b668e9203729e8cd666d7949c7cbfd8a49e0e9a21b",
  "outbox_state_now": "SENT",
  "provider_message_id": "sim-450077f652c3"
}
```
### 21. SEND_STOPPED_DUPLICATE
```json
{
  "email_type": "DECISION",
  "attempt": "simulated retry at 10:13",
  "idempotency_key": "bec121f99aa93ebfaa7a27b668e9203729e8cd666d7949c7cbfd8a49e0e9a21b",
  "detail": "idempotency_key already claimed by outbox_id=24 (state=SENT, provider_message_id=sim-450077f652c3, created 2026-10-24 10:06)"
}
```