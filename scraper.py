#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
上交所（SSE）官方 query.sse.com.cn 接口抓取沪深300相关 ETF 份额，合并导出 CSV。

接口说明：COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L，按 STAT_DATE（YYYY-MM-DD）查询。
反爬：必须带 Referer: http://www.sse.com.cn/，否则易返回 403。
"""

from __future__ import annotations

import json
import logging
import random
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# -----------------------------------------------------------------------------
# 日志
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 目标：9 只沪市 ETF（代码 -> 管理人/简称）
# -----------------------------------------------------------------------------
ETF_META: Dict[str, str] = {
    "510300": "华泰柏瑞",
    "510310": "易方达",
    "510320": "中金",
    "510350": "工银",
    "510330": "华夏",
    "510360": "广发",
    "510390": "平安",
    "510370": "兴业",
    "510380": "国寿",
}

# 列顺序（按代码排序，便于对比）
ETF_CODES_SORTED: List[str] = sorted(ETF_META.keys())

WORKDAY_REQUESTS = 120
OUTPUT_CSV = "etf_300_scale_data.csv"
# 增量模式：与「CSV 中最后交易日」起连续重叠的工作日根数（含最后一日共 N 日，用于覆盖修订）
INCREMENTAL_OVERLAP_WEEKDAYS = 5
# 合并后最多保留的交易日行数
MAX_STORED_ROWS = 120

SSE_QUERY_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_SQL_ID = "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L"

REQUEST_TIMEOUT = 45
MAX_RETRIES = 4
RETRY_BACKOFF_SEC = 2.0
REQUEST_INTERVAL_SEC = 1.0

# 随机 User-Agent 池（降低单一 UA 特征）
USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/122.0.0.0 Safari/537.36",
]


def build_sse_headers() -> Dict[str, str]:
    """
    上交所接口校验 Referer；缺省时常见 403。
    必须使用 http://www.sse.com.cn/（与官网一致，勿改为 https）。
    """
    return {
        "Referer": "http://www.sse.com.cn/",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }


def last_n_weekdays(n: int, end: date) -> List[date]:
    """从 end 起向前取 n 个工作日（周一～周五），不含周末。"""
    out: List[date] = []
    d = end
    max_scan = n * 3 + 14
    scanned = 0
    while len(out) < n and scanned < max_scan:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
        scanned += 1
    return list(reversed(out))


def subtract_weekdays(d: date, n: int) -> date:
    """从 d 起向前数 n 个工作日（不含 d 当天往前走的第 n 个 weekday）。"""
    cur = d
    left = n
    while left > 0:
        cur -= timedelta(days=1)
        if cur.weekday() < 5:
            left -= 1
    return cur


def read_existing_etf_csv(path: Path) -> Optional[pd.DataFrame]:
    """读取已存在的宽表 CSV；无效则返回 None。"""
    if not path.is_file():
        return None
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:  # noqa: BLE001
        return None
    if df.empty or "日期" not in df.columns:
        return None
    return df


def latest_date_in_etf_csv(df: pd.DataFrame) -> Optional[date]:
    s = pd.to_datetime(df["日期"], errors="coerce").dropna()
    if s.empty:
        return None
    return s.max().date()


def weekdays_between_inclusive(start: date, end: date) -> List[date]:
    """[start, end] 内所有工作日（周一～周五）。"""
    if start > end:
        return []
    out: List[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def plan_fetch_weekdays(today: date, existing: Optional[pd.DataFrame]) -> Tuple[List[date], str]:
    """
    决定本次要请求的日期列表。
    - 无历史：仍拉满 WORKDAY_REQUESTS 个工作日（冷启动）。
    - 有历史：从「最后日期往前 INCREMENTAL_OVERLAP_WEEKDAYS 个工作日」到今天，避免每天重复扫 120 天。
    """
    if existing is None:
        w = last_n_weekdays(WORKDAY_REQUESTS, today)
        return w, "全量(冷启动)"
    last_d = latest_date_in_etf_csv(existing)
    if last_d is None:
        w = last_n_weekdays(WORKDAY_REQUESTS, today)
        return w, "全量(无法解析历史日期)"
    # 从 last_d 起向前再数 (N-1) 个工作日作为起点，使 [start, last_d] 覆盖 N 个交易日
    back = max(0, INCREMENTAL_OVERLAP_WEEKDAYS - 1)
    start = subtract_weekdays(last_d, back)
    w = weekdays_between_inclusive(start, today)
    return w, f"增量(历史至 {last_d}，重叠约 {INCREMENTAL_OVERLAP_WEEKDAYS} 个交易日)"


def merge_etf_wide(old: Optional[pd.DataFrame], new_wide: pd.DataFrame) -> pd.DataFrame:
    """按日期合并，新数据覆盖同日旧数据；保留最近 MAX_STORED_ROWS 行（按日期降序）。"""
    if old is None or old.empty:
        out = new_wide.copy()
    else:
        out = pd.concat([old, new_wide], ignore_index=True)
    out["日期"] = pd.to_datetime(out["日期"], errors="coerce")
    out = out.dropna(subset=["日期"])
    out = out.sort_values("日期").drop_duplicates(subset=["日期"], keep="last")
    out = out.sort_values("日期", ascending=False).head(MAX_STORED_ROWS).reset_index(drop=True)
    out["日期"] = out["日期"].dt.strftime("%Y-%m-%d")
    return out


def fetch_sse_etf_scale_one_day(
    session: requests.Session,
    stat_date: date,
) -> pd.DataFrame:
    """
    调用上交所 commonQuery，返回当日全市场 ETF 份额表（仅解析 JSON）。
    失败或空结果返回空 DataFrame（含标准列）。
    """
    data_str = stat_date.strftime("%Y-%m-%d")
    params = {
        "isPagination": "true",
        "pageHelp.pageSize": "10000",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": "1",
        "sqlId": SSE_SQL_ID,
        "STAT_DATE": data_str,
    }

    last_error: Optional[BaseException] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = build_sse_headers()
            resp = session.get(
                SSE_QUERY_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" in ctype and not ctype.startswith("text/javascript"):
                payload = resp.json()
            else:
                text = resp.text.strip()
                if "jsonp" in text[:30].lower() or (text and text[0] not in "{["):
                    start = text.find("{")
                    end = text.rfind("}") + 1
                    if start >= 0 and end > start:
                        text = text[start:end]
                payload = json.loads(text)
            rows = payload.get("result")
            if not rows:
                logger.info("日期 %s 接口无 result（可能非披露日）", data_str)
                return pd.DataFrame(columns=["基金代码", "基金份额", "统计日期"])

            df = pd.DataFrame(rows)
            df = df.rename(
                columns={
                    "SEC_CODE": "基金代码",
                    "STAT_DATE": "统计日期",
                }
            )
            df["基金代码"] = df["基金代码"].astype(str).str.strip()
            # 与 akshare 一致：接口份额单位经 *10000 转为「份」
            df["基金份额"] = pd.to_numeric(df["TOT_VOL"], errors="coerce") * 10000
            df["统计日期"] = pd.to_datetime(df["统计日期"], errors="coerce").dt.normalize()
            return df[["基金代码", "基金份额", "统计日期"]]
        except requests.HTTPError as exc:
            last_error = exc
            logger.warning(
                "HTTP 错误 %s/%s 日期=%s: %s",
                attempt,
                MAX_RETRIES,
                data_str,
                exc,
            )
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            last_error = exc
            logger.warning(
                "解析失败 %s/%s 日期=%s: %s",
                attempt,
                MAX_RETRIES,
                data_str,
                exc,
            )
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "网络异常 %s/%s 日期=%s: %s",
                attempt,
                MAX_RETRIES,
                data_str,
                exc,
            )

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF_SEC * attempt)

    logger.error("日期 %s 最终失败: %s", data_str, last_error)
    return pd.DataFrame(columns=["基金代码", "基金份额", "统计日期"])


def collect_target_long(session: requests.Session, weekdays: List[date]) -> pd.DataFrame:
    """按工作日循环请求，每次 sleep 1s；只保留 9 只目标 ETF。"""
    pieces: List[pd.DataFrame] = []
    target_set = set(ETF_META.keys())

    for i, d in enumerate(weekdays):
        logger.info("请求进度 %s/%s: %s", i + 1, len(weekdays), d.isoformat())
        raw = fetch_sse_etf_scale_one_day(session, d)
        if not raw.empty:
            sub = raw[raw["基金代码"].isin(target_set)].copy()
            if not sub.empty:
                pieces.append(sub[["统计日期", "基金代码", "基金份额"]])

        if i < len(weekdays) - 1:
            time.sleep(REQUEST_INTERVAL_SEC)

    if not pieces:
        return pd.DataFrame(columns=["统计日期", "基金代码", "基金份额"])
    return pd.concat(pieces, ignore_index=True)


def long_to_output_csv(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    透视 -> 按工作日连续索引 reindex -> ffill -> 衍生列 -> 日期降序。
    """
    if long_df.empty:
        raise ValueError("未拉到任何目标 ETF 数据，请检查网络、日期或接口是否变更。")

    long_df = long_df.copy()
    long_df["统计日期"] = pd.to_datetime(long_df["统计日期"]).dt.normalize()

    wide = long_df.pivot_table(
        index="统计日期",
        columns="基金代码",
        values="基金份额",
        aggfunc="last",
    )

    for code in ETF_CODES_SORTED:
        if code not in wide.columns:
            wide[code] = pd.NA
    wide = wide[ETF_CODES_SORTED]

    wide = wide.sort_index()
    idx = pd.bdate_range(wide.index.min(), wide.index.max(), freq="B")
    wide = wide.reindex(idx)
    wide = wide.ffill()

    wide = wide.dropna(how="all")
    if wide.empty:
        raise ValueError("ffill 后宽表为空。")

    wide = wide.reset_index()
    wide = wide.rename(columns={wide.columns[0]: "日期"})

    rename_map = {c: f"份额_{c}_{ETF_META[c]}" for c in ETF_CODES_SORTED}
    wide = wide.rename(columns=rename_map)

    c300 = rename_map["510300"]
    c310 = rename_map["510310"]
    wide["华泰_易方达_合计"] = wide[c300] + wide[c310]
    wide["9大ETF_总份额"] = wide[list(rename_map.values())].sum(axis=1, min_count=9)

    wide["日期"] = pd.to_datetime(wide["日期"]).dt.strftime("%Y-%m-%d")
    wide = wide.sort_values("日期", ascending=False).reset_index(drop=True)
    return wide


def main() -> int:
    end = date.today()
    out_path = Path(OUTPUT_CSV)
    existing = read_existing_etf_csv(out_path)
    weekdays, mode = plan_fetch_weekdays(end, existing)
    logger.info(
        "模式：%s | 本次请求 %s 个工作日：%s → %s",
        mode,
        len(weekdays),
        weekdays[0].isoformat() if weekdays else "-",
        weekdays[-1].isoformat() if weekdays else "-",
    )

    try:
        with requests.Session() as session:
            long_df = collect_target_long(session, weekdays)
        if long_df.empty and existing is not None:
            logger.warning("本次未拉到新数据，保留原 CSV 不变。")
            return 0
        new_wide = long_to_output_csv(long_df)
        result = merge_etf_wide(existing, new_wide)
        result.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        logger.info("已写入 %s，行数=%s（合并后保留最近 %s 日）", OUTPUT_CSV, len(result), MAX_STORED_ROWS)
    except Exception as exc:  # noqa: BLE001
        logger.exception("运行失败: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
