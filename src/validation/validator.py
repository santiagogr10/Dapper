#!/usr/bin/env python3
"""
Validator CLI
Lee reglas desde configs/validation_rules.yaml y valida un CSV de entrada.
Salida:
 - valid.csv  (filas que pasan validación)
 - discarded.csv (filas descartadas con columna `discard_reason`)
"""
import argparse
import csv
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import yaml
import sys
import os

def load_rules(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg.get("fields", {})

def parse_boolean(value: Any) -> Tuple[bool, Any]:
    if pd.isna(value):
        return True, None
    if isinstance(value, bool):
        return True, value
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "y", "t"):
        return True, True
    if s in ("false", "0", "no", "n", "f"):
        return True, False
    return False, None

def parse_integer(value: Any) -> Tuple[bool, Any]:
    if pd.isna(value) or value == "":
        return True, None
    try:
        return True, int(value)
    except Exception:
        return False, None

def parse_string(value: Any) -> Tuple[bool, Any]:
    if pd.isna(value):
        return True, None
    return True, str(value)

def parse_date(value: Any, formats: List[str]) -> Tuple[bool, Any]:
    if pd.isna(value) or value == "":
        return True, None
    s = str(value).strip()
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return True, dt.date().isoformat()
        except Exception:
            continue
    return False, None

def validate_row(row: Dict[str, Any], rules: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str]:
    """
    Returns (is_valid_row, new_row, discard_reason)
    new_row may have None for invalid optional fields.
    If is_valid_row == False, discard_reason explains why.
    """
    new_row = dict(row)  # shallow copy
    for field, rule in rules.items():
        val = row.get(field, None)

        required = rule.get("required", False)
        ftype = rule.get("type", "string")
        max_length = rule.get("max_length", None)
        regex = rule.get("regex", None)
        allowed_values = rule.get("allowed_values", None)
        formats = rule.get("formats", ["%Y-%m-%d"])

        # Type parsing
        if ftype == "string":
            ok, parsed = parse_string(val)
        elif ftype == "integer":
            ok, parsed = parse_integer(val)
        elif ftype == "boolean":
            ok, parsed = parse_boolean(val)
        elif ftype == "date":
            ok, parsed = parse_date(val, formats)
        else:
            ok, parsed = parse_string(val)

        if not ok:
            if required:
                return False, new_row, f"{field}_type_invalid"
            else:
                new_row[field] = None
                continue

        # length check
        if max_length is not None and parsed is not None:
            try:
                if len(str(parsed)) > int(max_length):
                    if required:
                        return False, new_row, f"{field}_too_long ({len(str(parsed))}>{max_length})"
                    else:
                        new_row[field] = None
                        continue
            except Exception:
                # ignore length check on non-string-ish types
                pass

        # regex check
        if regex and parsed is not None:
            if not re.match(regex, str(parsed)):
                if required:
                    return False, new_row, f"{field}_regex_mismatch"
                else:
                    new_row[field] = None
                    continue

        # allowed values
        if allowed_values and parsed is not None:
            if str(parsed) not in [str(x) for x in allowed_values]:
                if required:
                    return False, new_row, f"{field}_not_allowed"
                else:
                    new_row[field] = None
                    continue

        new_row[field] = parsed

    return True, new_row, ""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", required=True, help="CSV input path")
    parser.add_argument("--rules", "-r", default="configs/validation_rules.yaml", help="Rules YAML")
    parser.add_argument("--output-dir", "-o", default="data/output", help="Output directory")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: input file not found: {args.input}", file=sys.stderr)
        sys.exit(2)
    if not os.path.exists(args.rules):
        print(f"ERROR: rules file not found: {args.rules}", file=sys.stderr)
        sys.exit(2)
    os.makedirs(args.output_dir, exist_ok=True)

    rules = load_rules(args.rules)
    df = pd.read_csv(args.input, dtype=str).fillna("")

    valid_rows = []
    discarded = []
    for _, r in df.iterrows():
        row = r.to_dict()
        ok, new_row, reason = validate_row(row, rules)
        if ok:
            valid_rows.append(new_row)
        else:
            # include original values + reason
            x = dict(row)
            x["discard_reason"] = reason
            discarded.append(x)

    valid_df = pd.DataFrame(valid_rows)
    discarded_df = pd.DataFrame(discarded)

    # write outputs
    valid_out = os.path.join(args.output_dir, "valid.csv")
    disc_out  = os.path.join(args.output_dir, "discarded.csv")

    if not valid_df.empty:
        valid_df.to_csv(valid_out, index=False)
    else:
        # create empty with header from rules keys if nothing valid
        cols = list(rules.keys())
        pd.DataFrame(columns=cols).to_csv(valid_out, index=False)

    if not discarded_df.empty:
        discarded_df.to_csv(disc_out, index=False)
    else:
        pd.DataFrame(columns=list(df.columns)+["discard_reason"]).to_csv(disc_out, index=False)

    # logging summary
    total = len(df)
    valid_n = 0 if valid_df.empty else len(valid_df)
    disc_n = 0 if discarded_df.empty else len(discarded_df)
    print(f"TOTAL={total}  VALID={valid_n}  DISCARDED={disc_n}")
    print(f"valid -> {valid_out}")
    print(f"discarded -> {disc_out}")

if __name__ == "__main__":
    main()
