# Seller Enrichment Pipeline

## Objective
Build a generalized Google Colab-ready notebook that processes semi-structured scraped seller artifact data from Excel/CSV files and converts it into structured outputs for:
- company details
- real product catalogue
- seller offerings
- debug / traceability

## Input Nature
Each row in the input is one scraped artifact or content object, NOT necessarily one product.

Examples:
- product candidate
- image
- brochure snippet
- webpage text
- PDF
- contact snippet
- company/about text
- policy/legal text
- marketing text
- project/portfolio content

## Current Problem
The current notebook generates outputs, but they are still poor quality.

### Main failures:
- wrong company names
- wrong company descriptions
- false positive products
- garbage offerings
- over-triggered hooks
- pipeline reconciliation issues

## Goal for next version
Create a much stronger, generalized, Colab-friendly notebook that:
- better routes artifacts
- correctly identifies real products
- improves company identity resolution
- improves offering eligibility logic
- keeps outputs internally consistent
- uses only free/open-source tools
