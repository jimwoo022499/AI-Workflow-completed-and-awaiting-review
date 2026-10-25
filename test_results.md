# Test results (final job run)

| # | Test | Result |
|---|---|---|
| 1 | Conflicting records -> founder acknowledged, lead alerted, no decision email | PASS |
| 2 | Lead approves -> approval email sent exactly once (repeat stopped) | PASS |
| 3 | Same inbound email delivered twice -> exactly one case, one acknowledgement | PASS |
| 4 | Lead rejects + provider timeout -> rejection drafted, outbox UNKNOWN, no auto-retry | PASS |
| 5 | Two concurrent sends of the same decision -> one SENT, one STOPPED_DUPLICATE | PASS |
| 6 | AI summary with fabricated claim -> validator rejects, template used | PASS |
| 7 | Spoofed sender -> nothing sent to sender, program lead alerted | PASS |
| 8 | 2-business-day deadline passes -> lead reminded, nothing sent automatically | PASS |

## Assertions

**1. Conflicting records -> founder acknowledged, lead alerted, no decision email**

- [x] reconciliation = CONFLICT → `CONFLICT`
- [x] acknowledgement SENT once to the founder, no status words → `['SENT']`
- [x] program lead alerted → `['jimmy.woo@accelerator.example']`
- [x] case waits for the lead (AWAITING_DECISION, on hold) → `['AWAITING_DECISION', True]`
- [x] decision email blocked until the lead decides → `BLOCKED_BY_GATE`

**2. Lead approves -> approval email sent exactly once (repeat stopped)**

- [x] records agree, but nothing sent before the lead decides → `AWAITING_DECISION`
- [x] approval email drafted and SENT exactly once → `['SENT']`
- [x] provider message id stored → `sim-67151ed4f907`
- [x] provider received exactly one delivery → `None`
- [x] repeat send STOPPED_DUPLICATE → `STOPPED_DUPLICATE`

**3. Same inbound email delivered twice -> exactly one case, one acknowledgement**

- [x] exactly one case for the message_id → `1`
- [x] second delivery flagged as duplicate, same case → `{'case_id': 'CASE-FFC064C3', 'duplicate': True}`
- [x] one inbound row, delivered_count = 2 → `{'n': 1, 'd': 2}`
- [x] only one acknowledgement and one lead alert → `None`

**4. Lead rejects + provider timeout -> rejection drafted, outbox UNKNOWN, no auto-retry**

- [x] rejection email drafted after the lead rejected → `Kelp Robotics: your Startup Program application`
- [x] outbox state UNKNOWN after timeout → `['UNKNOWN']`
- [x] retry sweeper skipped it → `None`
- [x] still UNKNOWN after sweeper (no auto-retry) → `UNKNOWN`
- [x] provider called exactly once → `1`
- [x] case routed to a human → `outbox UNKNOWN: provider timeout, delivery unconfirmed`

**5. Two concurrent sends of the same decision -> one SENT, one STOPPED_DUPLICATE**

- [x] outcomes = one SENT + one STOPPED_DUPLICATE → `['SENT', 'STOPPED_DUPLICATE']`
- [x] exactly one outbox row → `1`
- [x] provider received exactly one delivery → `None`

**6. AI summary with fabricated claim -> validator rejects, template used**

- [x] validator rejected the AI summary → `["known[0] value 'Approved' not found in review_queue.status (actual: ['Awaiting review'])", "known[0] claim asserts an approval decision: 'Review queue shows the startup has been approved'", 'records conflict/missing bu`
- [x] fabricated value 'Approved' flagged → `None`
- [x] template version used and logged → `TEMPLATE`
- [x] template lists the expected unknowns → `['Is the startup actually approved?', 'What does the tracker status mean?', 'Does one record supersede the other, or is one of them an error?', 'Which system is the source of truth for approval?']`

**7. Spoofed sender -> nothing sent to sender, program lead alerted**

- [x] case routed to the program lead (NEEDS_HUMAN) → `['NEEDS_HUMAN', 'sender tommy@soul-labs.example is not the founder email on file; domain resembles soul.example (possible spoof)']`
- [x] program lead alerted about the unverified sender → `None`
- [x] records never gathered or summarized → `None`
- [x] acknowledgement and decision emails both blocked → `['BLOCKED_BY_GATE', 'BLOCKED_BY_GATE']`
- [x] nothing ever delivered to the spoofed address → `0`

**8. 2-business-day deadline passes -> lead reminded, nothing sent automatically**

- [x] program lead reminded once the 2-day deadline passed → `Jimmy Woo (reminder)`
- [x] case still waits for the lead → `AWAITING_DECISION`
- [x] decision email still blocked (never sent automatically) → `BLOCKED_BY_GATE`
