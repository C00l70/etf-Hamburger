#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
沪深300指数（000300）日线爬虫：近 N 个交易日合并为一张 CSV。

主数据源：东方财富 push2his（多镜像依次尝试 + 完整浏览器头）。
备用源：腾讯证券 newfqkline（sh000300，前复权；部分衍生字段由 OHLC 推算或留空）。
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

INDEX_NAME = "沪深300"
INDEX_CODE = "000300"
SECID = "1.000300"
QQ_SYMBOL = "sh000300"

# 东财历史 K 线常见镜像（主域名被 RST/403 时可切换）
EM_KLINE_BASES: Tuple[str, ...] = (
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://7.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://19.push2his.eastmoney.com/api/qt/stock/kline/get",
    "https://90.push2his.eastmoney.com/api/qt/stock/kline/get",
)

QQ_KLINE_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"

CALENDAR_LOOKBACK = 200
TRADING_DAYS_KEEP = 120
# 增量拉指数 K 线时，相对 CSV 最后日期再往前多要的「自然日」重叠（覆盖修订、节假日边界）
INDEX_INCREMENTAL_OVERLAP_DAYS = 14

OUTPUT_CSV = "hs300_index_120d.csv"
MAX_RETRIES_PER_HOST = 3
RETRY_SLEEP = 2.0
TIMEOUT = 45

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


def _build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.8,
        status_forcelist=(502, 503, 504),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def em_headers() -> Dict[str, str]:
    ua = random.choice(USER_AGENTS)
    return {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Host": "push2his.eastmoney.com",
        "Referer": "https://quote.eastmoney.com/zs000300.html",
        "User-Agent": ua,
    }


def em_headers_for_url(url: str) -> Dict[str, str]:
    h = em_headers()
    try:
        from urllib.parse import urlparse

        host = urlparse(url).netloc.split("@")[-1]
        if host:
            h["Host"] = host
    except Exception:  # noqa: BLE001
        pass
    return h


def fetch_hs300_kline_em(
    beg: str,
    end: str,
    session: requests.Session,
) -> pd.DataFrame:
    """东方财富 K 线；beg/end 为 YYYYMMDD。"""
    params = {
        "secid": SECID,
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "0",
        "beg": beg,
        "end": end,
    }
    last_err: Optional[BaseException] = None

    for base in EM_KLINE_BASES:
        for attempt in range(1, MAX_RETRIES_PER_HOST + 1):
            try:
                r = session.get(
                    base,
                    params=params,
                    headers=em_headers_for_url(base),
                    timeout=TIMEOUT,
                )
                r.raise_for_status()
                data = r.json()
                block = data.get("data") or {}
                klines = block.get("klines") or []
                if not klines:
                    raise ValueError("klines 为空")
                df = pd.DataFrame([row.split(",") for row in klines])
                if df.shape[1] < 11:
                    raise ValueError(f"字段数异常: {df.shape[1]}")
                df = df.iloc[:, :11]
                df.columns = [
                    "日期",
                    "开盘",
                    "收盘",
                    "最高",
                    "最低",
                    "成交量",
                    "成交额",
                    "振幅",
                    "涨跌幅",
                    "涨跌额",
                    "换手率",
                ]
                for col in (
                    "开盘",
                    "收盘",
                    "最高",
                    "最低",
                    "成交量",
                    "成交额",
                    "振幅",
                    "涨跌幅",
                    "涨跌额",
                    "换手率",
                ):
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
                df = df.dropna(subset=["日期"])
                logger.info("东方财富 K 线成功: %s", base)
                return df
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                logger.warning(
                    "东财 %s 第 %s/%s 次失败: %s",
                    base.split("/")[2],
                    attempt,
                    MAX_RETRIES_PER_HOST,
                    exc,
                )
                if attempt < MAX_RETRIES_PER_HOST:
                    time.sleep(RETRY_SLEEP * attempt)

    if last_err:
        raise last_err
    raise RuntimeError("东方财富无可用响应")


def _parse_qq_jsonp(text: str) -> dict:
    """解析 kline_dayqfq=...JSON... / JSONP 包裹。"""
    start = text.find("{")
    if start < 0:
        raise ValueError("响应中无 JSON 对象")
    decoder = json.JSONDecoder()
    obj, _ = decoder.raw_decode(text[start:])
    return obj


def fetch_hs300_kline_qq(
    start_d: date,
    end_d: date,
    session: requests.Session,
) -> pd.DataFrame:
    """
    腾讯 newfqkline，指数 sh000300。
    返回列与东财对齐；腾讯仅提供成交额 amount，成交量/换手率置空，涨跌幅等由收盘价推算。
    """
    # param: symbol,day,start,end,maxbars,qfq
    param = f"{QQ_SYMBOL},day,{start_d.isoformat()},{end_d.isoformat()},1000,qfq"
    params = {
        "_var": "kline_dayqfq",
        "param": param,
        "r": str(random.random()),
    }
    headers = {
        "Referer": "https://gu.qq.com/sh000300/zs",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
    }
    last_err: Optional[BaseException] = None
    for attempt in range(1, MAX_RETRIES_PER_HOST + 1):
        try:
            r = session.get(
                QQ_KLINE_URL,
                params=params,
                headers=headers,
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            obj = _parse_qq_jsonp(r.text)
            code = obj.get("code")
            if code is not None and int(code) != 0:
                raise ValueError(f"业务 code={code}")
            data = (obj.get("data") or {}).get(QQ_SYMBOL) or {}
            rows = data.get("day") or data.get("qfqday")
            if not rows:
                raise ValueError("无 day/qfqday 数据")
            df = pd.DataFrame(rows)
            if df.shape[1] < 6:
                df = df.iloc[:, :6]
            df = df.iloc[:, :6]
            df.columns = ["日期", "开盘", "收盘", "最高", "最低", "成交额"]
            for col in ("开盘", "收盘", "最高", "最低", "成交额"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            df = df.dropna(subset=["日期"])

            prev_close = df["收盘"].shift(1)
            df["成交量"] = pd.NA
            df["振幅"] = (
                (df["最高"] - df["最低"]) / prev_close.replace(0, pd.NA) * 100
            )
            df["涨跌幅"] = (df["收盘"] - prev_close) / prev_close.replace(0, pd.NA) * 100
            df["涨跌额"] = df["收盘"] - prev_close
            df["换手率"] = pd.NA

            df = df[
                [
                    "日期",
                    "开盘",
                    "收盘",
                    "最高",
                    "最低",
                    "成交量",
                    "成交额",
                    "振幅",
                    "涨跌幅",
                    "涨跌额",
                    "换手率",
                ]
            ]
            logger.info("已使用腾讯行情备用源（前复权，成交量/换手率为空）")
            return df
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            logger.warning("腾讯行情 %s/%s: %s", attempt, MAX_RETRIES_PER_HOST, exc)
            if attempt < MAX_RETRIES_PER_HOST:
                time.sleep(RETRY_SLEEP * attempt)

    raise last_err if last_err else RuntimeError("腾讯行情失败")


def fetch_hs300_kline(beg: str, end_s: str, session: requests.Session) -> pd.DataFrame:
    try:
        return fetch_hs300_kline_em(beg, end_s, session)
    except Exception as em_exc:  # noqa: BLE001
        logger.warning("东方财富全部失败 (%s)，尝试腾讯备用源…", em_exc)
        start_d = date(int(beg[:4]), int(beg[4:6]), int(beg[6:8]))
        end_d = date(int(end_s[:4]), int(end_s[4:6]), int(end_s[6:8]))
        return fetch_hs300_kline_qq(start_d, end_d, session)


def read_existing_index_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.is_file():
        return None
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:  # noqa: BLE001
        return None
    if df.empty or "日期" not in df.columns:
        return None
    return df


def merge_index_kline(old: Optional[pd.DataFrame], raw_new: pd.DataFrame) -> pd.DataFrame:
    """按日期合并旧表与新拉取的 K 线，新数据覆盖同日；再交给 build_output_df 截断为 keep 条。"""
    if old is None or old.empty:
        return raw_new.copy()
    cols = [c for c in raw_new.columns if c in old.columns]
    if "日期" not in cols:
        return raw_new.copy()
    part_old = old[cols].copy()
    part_new = raw_new[cols].copy()
    merged = pd.concat([part_old, part_new], ignore_index=True)
    merged["日期"] = pd.to_datetime(merged["日期"], errors="coerce")
    merged = merged.dropna(subset=["日期"])
    merged = merged.sort_values("日期").drop_duplicates(subset=["日期"], keep="last")
    merged["日期"] = merged["日期"].dt.strftime("%Y-%m-%d")
    return merged


def build_output_df(raw: pd.DataFrame, keep: int) -> pd.DataFrame:
    if raw.empty:
        raise ValueError("未获取到任何指数 K 线数据。")
    work = raw.copy()
    work["_d"] = pd.to_datetime(work["日期"])
    work = work.sort_values("_d", ascending=True)
    work = work.drop(columns=["_d"])
    if len(work) < keep:
        logger.warning(
            "区间内仅 %s 个交易日，少于目标 %s，将全部写入 CSV。",
            len(work),
            keep,
        )
    tail = work.tail(keep).copy()
    tail.insert(0, "指数代码", INDEX_CODE)
    tail.insert(1, "指数名称", INDEX_NAME)
    tail = tail.sort_values("日期", ascending=False).reset_index(drop=True)
    return tail


def main() -> int:
    end = date.today()
    end_s = end.strftime("%Y%m%d")
    out_path = Path(OUTPUT_CSV)
    existing = read_existing_index_csv(out_path)

    if existing is not None:
        latest = pd.to_datetime(existing["日期"], errors="coerce").max()
        if pd.isna(latest):
            start = end - timedelta(days=CALENDAR_LOOKBACK)
            beg = start.strftime("%Y%m%d")
            mode = "全量(无法解析历史日期)"
        else:
            latest_d = latest.date()
            overlap_start = latest_d - timedelta(days=INDEX_INCREMENTAL_OVERLAP_DAYS)
            beg = overlap_start.strftime("%Y%m%d")
            mode = f"增量(CSV 末条 {latest_d}，向前重叠 {INDEX_INCREMENTAL_OVERLAP_DAYS} 自然日)"
    else:
        start = end - timedelta(days=CALENDAR_LOOKBACK)
        beg = start.strftime("%Y%m%d")
        mode = "全量(冷启动)"

    logger.info("拉取 %s(%s) 日线 [%s]: %s ~ %s", INDEX_NAME, INDEX_CODE, mode, beg, end_s)

    try:
        with _build_session() as session:
            raw_new = fetch_hs300_kline(beg, end_s, session)
        if raw_new.empty and existing is not None:
            logger.warning("本次未拉到指数 K 线，保留原 CSV。")
            return 0
        raw = merge_index_kline(existing, raw_new)
        out = build_output_df(raw, TRADING_DAYS_KEEP)
        out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        logger.info("已写入 %s，共 %s 行（合并后保留最近 %s 条，日期降序）", OUTPUT_CSV, len(out), TRADING_DAYS_KEEP)
    except Exception as exc:  # noqa: BLE001
        logger.exception("运行失败: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
