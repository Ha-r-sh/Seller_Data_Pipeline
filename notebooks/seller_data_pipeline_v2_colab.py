"""
Seller Enrichment Pipeline V2 (Colab-ready, open-source only)

How to use in Colab:
1) Upload this file and input xlsx/csv.
2) pip install pandas openpyxl rapidfuzz
3) Run: python seller_data_pipeline_v2_colab.py --input /path/to/input.xlsx --output_dir pipeline_outputs

Design goals:
- Each row is treated as one scraped artifact (not guaranteed product row).
- Conservative extraction + rich traceability.
- Multi-signal product validation with hard exclusion/override logic.
- Reconciliation to prevent silent product disappearance.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from rapidfuzz import fuzz


# =========================
# Config
# =========================
ARTIFACT_TAXONOMY = [
    "PRODUCT_CANDIDATE",
    "CATEGORY_OR_OFFERING",
    "COMPANY_INFO",
    "CONTACT_INFO",
    "POLICY_OR_LEGAL",
    "PROJECT_OR_PORTFOLIO",
    "BROCHURE_OR_DOCUMENT",
    "IMAGE_MEDIA",
    "MARKETING_OR_BRANDING",
    "GENERAL_WEBPAGE",
    "IRRELEVANT_OR_UNKNOWN",
]

SEMANTIC_ROLE_KEYWORDS = {
    "seller_id": ["unique identifier", "seller id", "seller_id", "uid", "id"],
    "source_url": ["source url", "website", "domain", "homepage", "source"],
    "found_on_page": ["found on page", "page url", "scraped from", "found on"],
    "item_type": ["item type", "type", "artifact type", "content type"],
    "item_name": ["name", "title", "heading", "item name", "product name"],
    "brand": ["brand", "brand name", "manufacturer"],
    "sku": ["sku", "model", "part number", "model number", "item code", "product code"],
    "artifact_link": ["link", "url", "href", "product url", "item url", "page link"],
    "description": ["description", "desc", "details", "about", "summary", "content", "text"],
    "price": ["price", "cost", "mrp", "amount", "rate"],
    "product_specifications": ["product specifications", "specifications", "specs", "technical"],
    "raw_text": ["raw text", "full text", "page text", "scraped text"],
    "page_title": ["page title", "meta title", "title tag"],
}

GENERIC_HEADING_BLOCKLIST = {
    "keep in touch", "location", "marketing office", "our journey", "inception",
    "expansion and growth", "digital transformation", "link scanning", "demat account",
    "about us", "contact us", "home", "read more",
}

HARD_NON_PRODUCT_PATTERNS = [
    r"\b(about us|our story|mission|vision|company profile|who we are)\b",
    r"\b(contact|get in touch|reach us|support|customer care|location|address)\b",
    r"\b(privacy|policy|terms|conditions|legal|cookies|refund|shipping)\b",
    r"\b(project|portfolio|case study|our work|timeline|journey|inception|milestone)\b",
    r"\b(marketing office|registered office|corporate office|footer|copyright)\b",
    r"\b(download brochure|catalogue pdf|brochure)\b",
]

CATEGORY_OFFERING_PATTERNS = [
    r"\b(products?|services?|solutions?|categories?|range|collection|portfolio)\b",
    r"\b(menu|service line|offering|segment|vertical|industry solutions?)\b",
]

SOURCE_TRUST_ORDER = {
    "about": 1,
    "contact": 2,
    "homepage": 3,
    "legal": 4,
    "generic": 5,
    "product": 6,
    "unknown": 7,
}


@dataclass
class PipelineConfig:
    product_threshold: float = 0.55
    product_override_threshold: float = 0.80
    offering_threshold: float = 0.60
    dedup_name_similarity: int = 90
    enable_optional_image_hook: bool = False
    enable_optional_ocr_hook: bool = False


def setup_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    return logging.getLogger("SellerPipelineV2")


log = setup_logger()


# =========================
# Utility functions
# =========================
def s(v) -> str:
    if pd.isna(v):
        return ""
    return str(v).strip()


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", s(text)).strip()


def normalize_url(url: str) -> str:
    url = s(url)
    if not url:
        return ""
    if not re.match(r"^https?://", url, flags=re.I):
        url = "https://" + url
    return url.rstrip("/")


def fuzzy_col_match(col: str, keywords: List[str], threshold: int = 72) -> bool:
    c = col.lower().strip()
    return any(fuzz.partial_ratio(c, k) >= threshold for k in keywords)


def detect_source_type(url: str) -> str:
    u = s(url).lower()
    if any(x in u for x in ["/about", "about-us", "who-we-are"]):
        return "about"
    if any(x in u for x in ["/contact", "get-in-touch", "reach-us"]):
        return "contact"
    if any(x in u for x in ["privacy", "terms", "legal", "policy"]):
        return "legal"
    if any(x in u for x in ["/product", "/shop", "/item", "/catalog"]):
        return "product"
    if re.match(r"^https?://[^/]+$", u):
        return "homepage"
    return "generic"


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-zA-Z0-9]+", s(text).lower())


# =========================
# Core pipeline steps
# =========================
def load_input(input_path: Path) -> pd.DataFrame:
    if input_path.suffix.lower() in {".xlsx", ".xls"}:
        sheets = pd.read_excel(input_path, sheet_name=None, dtype=str)
        frames = []
        for sheet_name, df in sheets.items():
            d = df.copy()
            d["_source_sheet"] = sheet_name
            d["_source_row"] = np.arange(len(d))
            frames.append(d)
        out = pd.concat(frames, ignore_index=True)
    elif input_path.suffix.lower() == ".csv":
        out = pd.read_csv(input_path, dtype=str)
        out["_source_sheet"] = "Sheet1"
        out["_source_row"] = np.arange(len(out))
    else:
        raise ValueError(f"Unsupported input type: {input_path.suffix}")

    out = out.replace(["nan", "NaN", "None", "NULL", "N/A", ""], np.nan)
    log.info("Loaded input rows=%s cols=%s", len(out), len(out.columns))
    return out


def infer_schema(df: pd.DataFrame) -> Dict[str, List[str]]:
    role_map: Dict[str, List[str]] = defaultdict(list)
    for role, keywords in SEMANTIC_ROLE_KEYWORDS.items():
        for col in df.columns:
            if fuzzy_col_match(col, keywords):
                role_map[role].append(col)
    log.info("Inferred roles: %s", {k: v for k, v in role_map.items() if v})
    return dict(role_map)


def first_non_empty(row: pd.Series, cols: List[str]) -> str:
    for c in cols:
        val = s(row.get(c, ""))
        if val:
            return val
    return ""


def join_non_empty(row: pd.Series, cols: List[str]) -> str:
    parts = [s(row.get(c, "")) for c in cols if s(row.get(c, ""))]
    return " | ".join(parts)


def normalize_rows(raw_df: pd.DataFrame, role_map: Dict[str, List[str]]) -> pd.DataFrame:
    rows = []
    for idx, row in raw_df.iterrows():
        item_name = join_non_empty(row, role_map.get("item_name", []))
        desc = join_non_empty(row, role_map.get("description", []))
        specs = join_non_empty(row, role_map.get("product_specifications", []))
        raw_text = join_non_empty(row, role_map.get("raw_text", []))
        page_title = join_non_empty(row, role_map.get("page_title", []))

        normalized = {
            "row_id": idx,
            "seller_id": first_non_empty(row, role_map.get("seller_id", [])),
            "source_url": normalize_url(first_non_empty(row, role_map.get("source_url", []))),
            "found_on_page": normalize_url(first_non_empty(row, role_map.get("found_on_page", []))),
            "item_type": first_non_empty(row, role_map.get("item_type", [])),
            "item_name": item_name,
            "brand": first_non_empty(row, role_map.get("brand", [])),
            "sku": first_non_empty(row, role_map.get("sku", [])),
            "artifact_link": normalize_url(first_non_empty(row, role_map.get("artifact_link", []))),
            "description": desc,
            "price": first_non_empty(row, role_map.get("price", [])),
            "product_specifications": specs,
            "page_title": page_title,
            "text_blob": normalize_text(" ".join([item_name, desc, specs, raw_text, page_title])),
            "source_type": detect_source_type(first_non_empty(row, role_map.get("found_on_page", []))),
            "_source_sheet": s(row.get("_source_sheet", "")),
            "_source_row": s(row.get("_source_row", "")),
        }
        rows.append(normalized)

    out = pd.DataFrame(rows)
    out["seller_id"] = out["seller_id"].replace("", pd.NA).fillna("UNKNOWN_SELLER")
    return out


def classify_artifact(row: pd.Series) -> str:
    text = (s(row["item_name"]) + " " + s(row["description"]) + " " + s(row["text_blob"])).lower()
    item_type = s(row["item_type"]).lower()
    link = s(row["artifact_link"]).lower()

    if any(x in item_type for x in ["image", "photo"]) or re.search(r"\.(jpg|jpeg|png|webp)$", link):
        return "IMAGE_MEDIA"
    if any(x in text for x in ["brochure", "catalogue", "catalog", "pdf", "download pdf"]) or ".pdf" in link:
        return "BROCHURE_OR_DOCUMENT"
    if re.search(r"\b(privacy|terms|policy|legal|cookie)\b", text):
        return "POLICY_OR_LEGAL"
    if re.search(r"\b(project|portfolio|our projects|case study)\b", text):
        return "PROJECT_OR_PORTFOLIO"
    if re.search(r"\b(contact|get in touch|phone|email|location|address|whatsapp)\b", text):
        return "CONTACT_INFO"
    if re.search(r"\b(about|company profile|who we are|established|founded)\b", text):
        return "COMPANY_INFO"
    if re.search(r"\b(brand story|campaign|tagline|follow us|subscribe)\b", text):
        return "MARKETING_OR_BRANDING"
    if any(re.search(p, text) for p in CATEGORY_OFFERING_PATTERNS):
        return "CATEGORY_OR_OFFERING"
    if any(x in item_type for x in ["product", "webpage", "listing"]) or re.search(r"\b(sku|model|buy|price|add to cart)\b", text):
        return "PRODUCT_CANDIDATE"
    if s(row["found_on_page"]):
        return "GENERAL_WEBPAGE"
    return "IRRELEVANT_OR_UNKNOWN"


def resolve_company_identity(group: pd.DataFrame) -> Dict[str, str]:
    candidates = []
    for _, r in group.iterrows():
        source_rank = SOURCE_TRUST_ORDER.get(s(r["source_type"]), 7)
        for candidate in [s(r["brand"]), s(r["item_name"]), s(r["page_title"])]:
            c = normalize_text(candidate)
            if not c:
                continue
            c_l = c.lower()
            if c_l in GENERIC_HEADING_BLOCKLIST:
                continue
            if len(c_l) < 3 or len(c_l) > 90:
                continue
            if any(re.search(p, c_l) for p in HARD_NON_PRODUCT_PATTERNS):
                continue
            score = max(0.0, 1.0 - (source_rank - 1) * 0.12)
            if re.search(r"\b(private limited|pvt|limited|llp|inc|llc)\b", c_l):
                score += 0.25
            if re.search(r"\b(link scanning|html dom|json-ld)\b", c_l):
                score -= 0.30
            candidates.append((c, score, s(r["source_type"]), int(r["row_id"])))

    if not candidates:
        website = s(group["source_url"].iloc[0])
        domain = re.sub(r"^https?://(www\.)?", "", website).split("/")[0]
        fallback_name = domain.split(".")[0].replace("-", " ").title() if domain else ""
        return {
            "registered_company_name": "",
            "brand_name": fallback_name,
            "identity_confidence": 0.20,
            "identity_evidence": "fallback_domain",
        }

    agg = defaultdict(lambda: {"score": 0.0, "evidence": []})
    for c, score, src, row_id in candidates:
        key = c.lower()
        agg[key]["score"] += score
        agg[key]["evidence"].append(f"row={row_id}|src={src}|score={score:.2f}")

    best_key = max(agg, key=lambda k: agg[k]["score"])
    best_name = next(c for c, *_ in candidates if c.lower() == best_key)
    confidence = min(1.0, agg[best_key]["score"] / max(1.0, len(agg[best_key]["evidence"])))

    return {
        "registered_company_name": best_name if re.search(r"\b(private limited|pvt|limited|llp|inc|llc)\b", best_key) else "",
        "brand_name": best_name,
        "identity_confidence": round(confidence, 3),
        "identity_evidence": " || ".join(agg[best_key]["evidence"][:6]),
    }


def company_description_from_group(group: pd.DataFrame) -> str:
    trusted = group[group["source_type"].isin(["about", "homepage", "contact", "legal"])].copy()
    trusted = trusted[~trusted["artifact_class"].isin(["PRODUCT_CANDIDATE", "PROJECT_OR_PORTFOLIO"])].copy()
    if trusted.empty:
        return ""
    candidates = []
    for _, r in trusted.iterrows():
        t = normalize_text(r["text_blob"])[:700]
        if len(t) < 60:
            continue
        if any(re.search(p, t.lower()) for p in [r"\bprivacy\b", r"\bterms\b", r"\bcookie\b"]):
            continue
        candidates.append(t)
    if not candidates:
        return ""
    return max(candidates, key=len)[:500]


def product_signals(row: pd.Series) -> Dict[str, float]:
    name = s(row["item_name"])
    desc = s(row["description"])
    specs = s(row["product_specifications"])
    price = s(row["price"])
    sku = s(row["sku"])
    text = s(row["text_blob"]).lower()
    found = s(row["found_on_page"]).lower()

    sig = {
        "name_specific": float(len(tokenize(name)) >= 2 and len(name) <= 140),
        "has_sku": float(bool(re.search(r"[A-Za-z]{1,5}[-_]?\d{2,}", sku or name))),
        "has_price": float(bool(re.search(r"(₹|rs\.?|inr|\$|usd|eur)\s?\d", (price + " " + text).lower()))),
        "has_specs": float(len(specs) >= 15 or bool(re.search(r"\b(spec|dimension|weight|material|capacity|voltage)\b", text))),
        "product_cta": float(bool(re.search(r"\b(add to cart|buy now|order now|in stock)\b", text))),
        "product_url": float(bool(re.search(r"/(product|shop|item|p)/", found))),
        "desc_quality": float(len(tokenize(desc)) >= 8),
        "category_context": float(row["artifact_class"] in {"PRODUCT_CANDIDATE", "IMAGE_MEDIA"}),
    }
    return sig


def non_product_block(row: pd.Series) -> Tuple[bool, List[str]]:
    txt = (s(row["item_name"]) + " " + s(row["description"]) + " " + s(row["text_blob"]).lower())
    reasons = []
    if s(row["item_name"]).lower() in GENERIC_HEADING_BLOCKLIST:
        reasons.append("generic_heading")
    if row["artifact_class"] in {
        "COMPANY_INFO", "CONTACT_INFO", "POLICY_OR_LEGAL", "PROJECT_OR_PORTFOLIO", "MARKETING_OR_BRANDING"
    }:
        reasons.append(f"artifact_class={row['artifact_class']}")
    for p in HARD_NON_PRODUCT_PATTERNS:
        if re.search(p, txt, re.I):
            reasons.append(f"matched_non_product_pattern:{p[:24]}")
            break
    return (len(reasons) > 0), reasons


def validate_products(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        sig = product_signals(r)
        weighted_score = (
            0.17 * sig["name_specific"]
            + 0.17 * sig["has_sku"]
            + 0.16 * sig["has_price"]
            + 0.13 * sig["has_specs"]
            + 0.10 * sig["product_cta"]
            + 0.10 * sig["product_url"]
            + 0.09 * sig["desc_quality"]
            + 0.08 * sig["category_context"]
        )

        blocked, block_reasons = non_product_block(r)
        strong_evidence = sig["has_sku"] + sig["has_price"] + sig["has_specs"] + sig["product_cta"] >= 2.0

        is_product = weighted_score >= cfg.product_threshold and (not blocked or (strong_evidence and weighted_score >= cfg.product_override_threshold))

        rows.append(
            {
                "row_id": int(r["row_id"]),
                "seller_id": s(r["seller_id"]),
                "item_name": s(r["item_name"]),
                "description": s(r["description"]),
                "artifact_class": s(r["artifact_class"]),
                "is_actual_product": bool(is_product),
                "product_confidence_score": round(float(weighted_score), 3),
                "hard_exclusion_triggered": blocked,
                "hard_exclusion_reasons": " | ".join(block_reasons),
                "signal_breakdown": json.dumps(sig),
            }
        )
    return pd.DataFrame(rows)


def build_product_catalogue(df: pd.DataFrame, validation: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    merged = df.merge(validation[["row_id", "is_actual_product", "product_confidence_score"]], on="row_id", how="left")
    prod = merged[merged["is_actual_product"]].copy()
    prod["normalized_name"] = prod["item_name"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()

    keep = []
    for sid, grp in prod.groupby("seller_id"):
        chosen = []
        for _, row in grp.sort_values("product_confidence_score", ascending=False).iterrows():
            nm = s(row["normalized_name"])
            if not nm:
                continue
            duplicate = any(fuzz.ratio(nm, ex) >= cfg.dedup_name_similarity for ex in chosen)
            if not duplicate:
                chosen.append(nm)
                keep.append(row)
    if keep:
        out = pd.DataFrame(keep)
    else:
        out = pd.DataFrame(columns=prod.columns)

    out = out.reset_index(drop=True)
    out["product_id"] = [f"P{i+1:04d}" for i in range(len(out))]
    cols = [
        "seller_id", "product_id", "row_id", "item_name", "brand", "sku", "price",
        "description", "product_specifications", "artifact_link", "found_on_page", "product_confidence_score",
    ]
    return out[cols]


def build_seller_offerings(df: pd.DataFrame, validation: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    merged = df.merge(validation[["row_id", "is_actual_product", "product_confidence_score"]], on="row_id", how="left")
    candidates = merged[~merged["is_actual_product"]].copy()

    offerings = []
    for _, r in candidates.iterrows():
        txt = (s(r["item_name"]) + " " + s(r["description"]) + " " + s(r["text_blob"]).lower())
        if r["artifact_class"] not in {"CATEGORY_OR_OFFERING", "GENERAL_WEBPAGE"}:
            continue
        if any(re.search(p, txt, re.I) for p in HARD_NON_PRODUCT_PATTERNS):
            continue
        category_signal = 1.0 if any(re.search(p, txt, re.I) for p in CATEGORY_OFFERING_PATTERNS) else 0.0
        name_signal = float(2 <= len(tokenize(s(r["item_name"]))) <= 8)
        not_generic = float(s(r["item_name"]).lower() not in GENERIC_HEADING_BLOCKLIST)
        score = 0.45 * category_signal + 0.30 * name_signal + 0.25 * not_generic
        if score >= cfg.offering_threshold:
            offerings.append(
                {
                    "seller_id": s(r["seller_id"]),
                    "row_id": int(r["row_id"]),
                    "offering_name": s(r["item_name"]),
                    "offering_description": s(r["description"])[:300],
                    "source_url": s(r["source_url"]),
                    "offering_confidence": round(score, 3),
                }
            )
    out = pd.DataFrame(offerings)
    if out.empty:
        return out
    out = out.drop_duplicates(subset=["seller_id", "offering_name"]).reset_index(drop=True)
    out["offering_id"] = [f"O{i+1:04d}" for i in range(len(out))]
    return out[["seller_id", "offering_id", "row_id", "offering_name", "offering_description", "source_url", "offering_confidence"]]


def build_company_details(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for sid, grp in df.groupby("seller_id"):
        ident = resolve_company_identity(grp)
        desc = company_description_from_group(grp)
        emails = sorted(set(re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}", " ".join(grp["text_blob"].fillna("")))))
        phones = sorted(set(re.findall(r"(?:\+?\d[\d\-\s]{7,}\d)", " ".join(grp["text_blob"].fillna("")))))
        website = s(grp["source_url"].iloc[0])
        rec = {
            "seller_id": sid,
            "registered_company_name": ident["registered_company_name"],
            "brand_name": ident["brand_name"],
            "website": website,
            "primary_email": emails[0] if emails else "",
            "alternate_emails": "; ".join(emails[1:4]) if len(emails) > 1 else "",
            "primary_phone": phones[0] if phones else "",
            "alternate_phones": "; ".join(phones[1:4]) if len(phones) > 1 else "",
            "company_description": desc,
            "company_identity_confidence": ident["identity_confidence"],
            "company_identity_evidence": ident["identity_evidence"],
        }
        records.append(rec)
    return pd.DataFrame(records)


def build_reconciliation(df: pd.DataFrame, validation: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    validated_yes = validation[validation["is_actual_product"]][["row_id", "seller_id", "item_name", "product_confidence_score"]].copy()
    product_row_ids = set(products["row_id"].tolist()) if not products.empty else set()
    rows = []
    for _, r in validated_yes.iterrows():
        row_id = int(r["row_id"])
        if row_id in product_row_ids:
            rows.append({
                "row_id": row_id,
                "seller_id": s(r["seller_id"]),
                "status": "INCLUDED_IN_PRODUCT_EXPORT",
                "reason": "passed_validation_and_dedup",
            })
        else:
            rows.append({
                "row_id": row_id,
                "seller_id": s(r["seller_id"]),
                "status": "EXCLUDED_AFTER_VALIDATION",
                "reason": "deduplicated_or_missing_name",
            })
    return pd.DataFrame(rows)


def run_pipeline(input_path: Path, output_dir: Path, cfg: PipelineConfig) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = load_input(input_path)
    role_map = infer_schema(raw)
    clean = normalize_rows(raw, role_map)

    clean["artifact_class"] = clean.apply(classify_artifact, axis=1)
    class_counts = Counter(clean["artifact_class"])
    log.info("Artifact class distribution: %s", dict(class_counts))

    validation = validate_products(clean, cfg)
    products = build_product_catalogue(clean, validation, cfg)
    offerings = build_seller_offerings(clean, validation, cfg)
    company = build_company_details(clean)
    reconcile = build_reconciliation(clean, validation, products)

    clean.to_excel(output_dir / "cleaned_intermediate_data.xlsx", index=False)
    clean[[
        "row_id", "seller_id", "source_url", "found_on_page", "item_type", "item_name",
        "description", "artifact_class", "source_type",
    ]].to_excel(output_dir / "artifact_classification_output.xlsx", index=False)
    validation.to_excel(output_dir / "product_validation_debug_output.xlsx", index=False)
    products.to_excel(output_dir / "product_catalogue_output.xlsx", index=False)
    offerings.to_excel(output_dir / "seller_offerings_output.xlsx", index=False)
    company.to_excel(output_dir / "company_details_output.xlsx", index=False)
    reconcile.to_excel(output_dir / "reconciliation_log_output.xlsx", index=False)

    summary = {
        "run_timestamp_utc": datetime.utcnow().isoformat(),
        "input_rows": int(len(clean)),
        "artifact_class_distribution": dict(class_counts),
        "validated_products": int(validation["is_actual_product"].sum()),
        "exported_products": int(len(products)),
        "exported_offerings": int(len(offerings)),
        "company_records": int(len(company)),
        "reconciliation_rows": int(len(reconcile)),
        "optional_hooks": {
            "enable_optional_image_hook": cfg.enable_optional_image_hook,
            "enable_optional_ocr_hook": cfg.enable_optional_ocr_hook,
        },
    }
    (output_dir / "processing_summary.json").write_text(json.dumps(summary, indent=2))

    log.info("Pipeline completed. Output directory: %s", output_dir)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seller enrichment pipeline V2")
    p.add_argument("--input", required=True, help="Path to input xlsx/csv")
    p.add_argument("--output_dir", default="pipeline_outputs_v2", help="Output directory")
    p.add_argument("--product_threshold", type=float, default=0.55)
    p.add_argument("--product_override_threshold", type=float, default=0.80)
    p.add_argument("--offering_threshold", type=float, default=0.60)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = PipelineConfig(
        product_threshold=args.product_threshold,
        product_override_threshold=args.product_override_threshold,
        offering_threshold=args.offering_threshold,
    )
    run_pipeline(Path(args.input), Path(args.output_dir), cfg)


if __name__ == "__main__":
    main()
