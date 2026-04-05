#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将爬虫产出的两份 CSV 按日期对齐合并，生成可视化页面使用的 data.json。

输入（默认与脚本同目录）：
  - etf_300_scale_data.csv  （9 只 ETF 份额列）
  - hs300_index_120d.csv    （沪深300 日线，取「收盘」）

输出：
  - data.json
    {
      "generated_at": "...",
      "source_files": {...},
      "fund_labels": ["华泰柏瑞", ...],  // 与 fund_1..fund_9 顺序一致
      "series": [
        { "date": "YYYY-MM-DD", "index_close": float, "fund_1": ..., ... "fund_9": ... }
      ]
    }

仅保留「两个数据源均存在该日期」的行，并按日期升序排列。
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# 与 etf_300_scale_data.csv 中 9 列份额顺序严格一致（510300 → 510390）
ETF_SHARE_COLUMNS = [
    "份额_510300_华泰柏瑞",
    "份额_510310_易方达",
    "份额_510320_中金",
    "份额_510330_华夏",
    "份额_510350_工银",
    "份额_510360_广发",
    "份额_510370_兴业",
    "份额_510380_国寿",
    "份额_510390_平安",
]

FUND_LABELS = [
    "华泰柏瑞",
    "易方达",
    "中金",
    "华夏",
    "工银",
    "广发",
    "兴业",
    "国寿",
    "平安",
]

DEFAULT_ETF_CSV = "etf_300_scale_data.csv"
DEFAULT_INDEX_CSV = "hs300_index_120d.csv"
OUTPUT_JSON = "data.json"


def load_merged_series(
    etf_path: Path,
    index_path: Path,
) -> tuple[list[dict], dict]:
    etf = pd.read_csv(etf_path, encoding="utf-8-sig")
    idx = pd.read_csv(index_path, encoding="utf-8-sig")

    if "日期" not in etf.columns:
        raise ValueError(f"{etf_path} 缺少「日期」列")
    if "日期" not in idx.columns or "收盘" not in idx.columns:
        raise ValueError(f"{index_path} 需包含「日期」「收盘」列")

    missing = [c for c in ETF_SHARE_COLUMNS if c not in etf.columns]
    if missing:
        raise ValueError(f"ETF CSV 缺少列: {missing}")

    etf = etf.copy()
    etf["日期"] = pd.to_datetime(etf["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    idx = idx.copy()
    idx["日期"] = pd.to_datetime(idx["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    idx = idx[["日期", "收盘"]].rename(columns={"收盘": "index_close"})

    merged = pd.merge(etf[["日期"] + ETF_SHARE_COLUMNS], idx, on="日期", how="inner")
    merged = merged.dropna(subset=["index_close"])
    merged = merged.sort_values("日期", ascending=True).reset_index(drop=True)

    series: list[dict] = []
    for _, row in merged.iterrows():
        item = {
            "date": row["日期"],
            "index_close": float(row["index_close"]),
        }
        for i, col in enumerate(ETF_SHARE_COLUMNS, start=1):
            v = row[col]
            item[f"fund_{i}"] = float(v) if pd.notna(v) else None
        series.append(item)

    meta = {
        "rows_etf": len(etf),
        "rows_index": len(idx),
        "rows_merged": len(series),
    }
    return series, meta


def main() -> int:
    root = Path(__file__).resolve().parent
    etf_path = root / DEFAULT_ETF_CSV
    index_path = root / DEFAULT_INDEX_CSV
    out_path = root / OUTPUT_JSON

    if not etf_path.is_file():
        print(f"错误: 未找到 {etf_path}", file=sys.stderr)
        return 1
    if not index_path.is_file():
        print(f"错误: 未找到 {index_path}", file=sys.stderr)
        return 1

    try:
        series, merge_meta = load_merged_series(etf_path, index_path)
    except Exception as exc:  # noqa: BLE001
        print(f"合并失败: {exc}", file=sys.stderr)
        return 1

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_files": {
            "etf_scale": DEFAULT_ETF_CSV,
            "hs300_index": DEFAULT_INDEX_CSV,
        },
        "fund_labels": FUND_LABELS,
        "merge_stats": merge_meta,
        "series": series,
    }

    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"已写入 {out_path}，合并行数: {len(series)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
