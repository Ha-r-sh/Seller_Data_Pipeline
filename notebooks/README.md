# Notebooks / Code

- `seller_data_pipeline_V1.4.ipynb` — previous notebook version (kept for reference).
- `seller_data_pipeline_v2_colab.py` — improved Colab-ready pipeline code (V2).

## Run V2 in Colab
```bash
pip install pandas openpyxl rapidfuzz
python seller_data_pipeline_v2_colab.py \
  --input /content/Sample\ Product\ or\ Catalog\ URLs.xlsx \
  --output_dir /content/pipeline_outputs_v2
```

Outputs written:
- `cleaned_intermediate_data.xlsx`
- `artifact_classification_output.xlsx`
- `company_details_output.xlsx`
- `product_validation_debug_output.xlsx`
- `product_catalogue_output.xlsx`
- `seller_offerings_output.xlsx`
- `reconciliation_log_output.xlsx`
- `processing_summary.json`
