# ADR 007 — Rules + LLM Hybrid Diagnosis

**Context:** Need to decide the AI layer's architecture: rules, LLM, or both.

**Decision:** Hybrid — deterministic rule engine runs first and always; optional LLM refines/explores on top of the same evidence substrate, gated by a mechanical evidence-resolution check. **The LLM proposes/refines the complete diagnosis** — `IncidentType`, `Severity`, `AffectedNodes`, `Confidence`, and claims — not merely narrative text layered on top of rule-engine-owned fields (`docs/ai-design.md`'s "LLM client interface" section spells out why: Phase 9 evaluates classification accuracy, affected-node accuracy, and confidence calibration, none of which would be meaningful metrics if the LLM never actually produced the fields being scored). The rule engine's own candidate (if any) is passed to the LLM as context to confirm, refine, or override — it isn't a hard ceiling on what the LLM's response can contain.

**Alternatives considered:**
- **Rules only:** deterministic, explainable, zero hallucination risk — but brittle, catches only patterns explicitly encoded, produces alerts not narratives.
- **LLM only ("logs → LLM → explanation"):** flexible, handles novel/compound failures, but hallucination-prone if not grounded and hard to evaluate.

**Why hybrid:** rules give a fast floor achieving a 100% pass rate on the defined validation scenarios (a precise, defensible claim — not "100% reliable" in the unqualified general sense) for known patterns, plus guaranteed-grounded evidence; the LLM adds narrative richness and handles ambiguous cases on top of the same grounded substrate, rather than reasoning freely over raw data.

**Trade-offs:** more engineering surface area than either alone — justified because demonstrating an understood tradeoff is the actual point of including AI in this project at all.

**Consequences:** the AI layer's evaluation (Phase 9, `docs/ai-design.md`) can report **accepted-output** evidence-validity as a hard 100% metric (a property of the validator, which rejects anything unresolvable, rather than a claim that the LLM itself never tries to hallucinate — that separate rate is tracked too) specifically because the rule-engine-first, validator-authoritative design makes the hard guarantee achievable.
