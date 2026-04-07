# CHANGELOG

## 2026-04-07 — Pipeline V2 refresh

### What changed
- Added a new Colab-ready pipeline implementation at `notebooks/seller_data_pipeline_v2_colab.py`.
- Implemented generalized schema inference and row normalization for semi-structured artifact exports.
- Replaced coarse routing with richer artifact taxonomy (`PRODUCT_CANDIDATE`, `COMPANY_INFO`, `POLICY_OR_LEGAL`, `PROJECT_OR_PORTFOLIO`, etc.).
- Added explicit company identity resolution with candidate ranking and generic-heading rejection.
- Added multi-signal real-product validation with interpretable signal breakdown.
- Added hard non-product exclusion with controlled override only when strong contrary product evidence exists.
- Added offering-eligibility logic so that “not product” does not automatically become “offering”.
- Added explicit reconciliation log to ensure validated products never disappear silently after export.
- Added processing summary with counts for validation vs export consistency.

### Why this changed
The previous outputs showed quality and integrity issues:
- wrong company names/descriptions from noisy website snippets,
- non-products entering product catalogue,
- garbage offerings from timeline/contact/marketing fragments,
- weak traceability when validated products disappeared after later steps.

### Failure modes targeted
- Wrong company names (`Link Scanning`, `Keep in touch`, `Location`, etc.)
- Wrong company descriptions (policy/history/product snippets)
- False positive products (policy/contact/branding/project rows)
- Garbage offerings (`Our Journey`, `Inception`, etc.)
- Reconciliation failures between validation and final outputs
- Overly broad hook behavior (kept optional hooks explicitly gated)
