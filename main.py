# pip install requests pandas python-dateutil

import os
import sys
import time
import random
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # ✅ 올바른 위치에서 임포트

# -------------------- 로깅 --------------------
def setup_logging(token_name: str) -> logging.Logger:
    """토큰명 기반 로깅 설정 (콘솔+파일)"""
    log_filename = f'{(token_name or "token").lower()}_analysis.log'

    # 기존 핸들러 제거
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ],
        force=True
    )
    return logging.getLogger(token_name.lower() or __name__)

UTC = timezone.utc
KST = timezone(timedelta(hours=9))
_modlog = logging.getLogger(__name__)  # 모듈 유틸용 로거(클래스 밖)

# -------------------- 설정 --------------------
@dataclass
class Config:
    # 분석 파라미터
    MIN_CLUSTER_SIZE: int = 3
    CLUSTER_WINDOW_MINUTES: int = 120
    HIGH_FAIL_RATE_THRESHOLD: float = 0.8
    CONSECUTIVE_FAIL_THRESHOLD: int = 10

    # 네트워킹/성능
    MAX_RETRIES: int = 3
    BATCH_SIZE: int = 1000
    REQUEST_DELAY: float = 0.25       # rps 4~5 권장
    REQUEST_TIMEOUT: int = 15
    USE_RECEIPT_GAS: bool = True      # 실패 Top-N은 receipt로 가스 보정
    ENRICH_RECEIPT_TOPN: int = 100

    # NOTE: HOLDER_LIMIT, TOKEN_ADDRESS 등은 코드에 하드코딩하지 않음(환경변수 필수)
    HOLDER_LIMIT: Optional[int] = None

# -------------------- 유틸 --------------------
def parse_window_from_env() -> Tuple[Optional[datetime], Optional[datetime], str]:
    """
    시간 윈도우를 환경변수에서 읽어 UTC로 반환.
      - WINDOW_START_KST / WINDOW_END_KST  (KST 해석)
      - WINDOW_START_UTC / WINDOW_END_UTC  (UTC 해석)
      - 없으면 (None, None, "")
    """
    fmt = "%Y-%m-%d %H:%M:%S"
    ks = os.getenv("WINDOW_START_KST", "").strip()
    ke = os.getenv("WINDOW_END_KST", "").strip()
    us = os.getenv("WINDOW_START_UTC", "").strip()
    ue = os.getenv("WINDOW_END_UTC", "").strip()

    if ks and ke:
        try:
            start_kst = datetime.strptime(ks, fmt).replace(tzinfo=KST)
            end_kst   = datetime.strptime(ke, fmt).replace(tzinfo=KST)
            return start_kst.astimezone(UTC), end_kst.astimezone(UTC), "KST"
        except Exception as e:
            _modlog.warning(f"Failed to parse KST window: {e}")

    if us and ue:
        try:
            start_utc = datetime.strptime(us, fmt).replace(tzinfo=UTC)
            end_utc   = datetime.strptime(ue, fmt).replace(tzinfo=UTC)
            return start_utc, end_utc, "UTC"
        except Exception as e:
            _modlog.warning(f"Failed to parse UTC window: {e}")

    return None, None, ""

def within_window(ts_utc: datetime, start_utc: Optional[datetime], end_utc: Optional[datetime]) -> bool:
    if start_utc and ts_utc < start_utc:
        return False
    if end_utc and ts_utc > end_utc:
        return False
    return True

# -------------------- 메인 클래스 --------------------
class EtherscanAnalyzer:
    def __init__(self, api_key: str, config: Config = None, logger: Optional[logging.Logger] = None):
        self.api_key = api_key or os.getenv("ETHERSCAN_API_KEY")
        if not self.api_key or self.api_key == "YOUR_API_KEY":
            raise ValueError("ETHERSCAN_API_KEY 환경변수가 설정되지 않았습니다.")

        self.config = config or Config()
        self.logger = logger or logging.getLogger(__name__)
        self.base_url = "https://api.etherscan.io/api"         # v1
        self.base_url_v2 = "https://api.etherscan.io/v2/api"   # v2

        # 세션 + Retry
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"])
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        # 시간 윈도우(UTC) 설정
        self.window_start_utc, self.window_end_utc, self.window_kind = parse_window_from_env()
        if self.window_start_utc or self.window_end_utc:
            s = self.window_start_utc.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S %Z") if self.window_start_utc else "-"
            e = self.window_end_utc.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S %Z") if self.window_end_utc else "-"
            self.logger.info(f"Applied time window [{self.window_kind}]: {s} ~ {e}")

        # 라우터 주소들
        self.routers: Set[str] = {
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2
            "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3
            "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3 Router 2
            "0x1111111254fb6c44bac0bed2854e76f90643097d",  # 1inch v4
            "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch v5
            "0xdef1c0ded9bec7f1a1670819833240f027b25eff",  # 0x
            "0x881d40237659c251811cec9c364ef91dc08d300c",  # MetaMask swap
        }
        suspect_router = os.getenv("EXTRA_ROUTER", "").strip().lower()
        if suspect_router.startswith("0x") and len(suspect_router) == 42:
            self.routers.add(suspect_router)
            self.logger.info(f"Added EXTRA_ROUTER: {suspect_router}")

        # 함수 시그니처 매핑(확장)
        self.sig_map: Dict[str, str] = {
            # Uniswap V2
            "0x18cbafe5": "swapExactTokensForETH",
            "0x791ac947": "swapExactTokensForETHSupportingFeeOnTransferTokens",
            "0x4a25d94a": "swapTokensForExactETH",
            "0x7ff36ab5": "swapExactETHForTokens",
            "0xfb3bdb41": "swapETHForExactTokens",
            "0x38ed1739": "swapExactTokensForTokens",
            "0x8803dbee": "swapTokensForExactTokens",
            "0xb6f9de95": "swapExactETHForTokensSupportingFeeOnTransferTokens",
            # Uniswap V3 (간단 표기 — 실제 방향성은 path 필요)
            "0x414bf389": "exactInputSingle",   # 0x414bf389 옛/새 버전 차이 있음
            "0xb858183f": "exactInput",
            # 1inch/0x 등
            "0x12aa3caf": "1inch.swap",
            "0x0502b1c5": "1inch.unoswap",
            "0x2e95b6c8": "1inch.unoswap",
            # ERC-20
            "0xa9059cbb": "erc20.transfer",
            "0x095ea7b3": "erc20.approve",
        }

        # 실행 중 토큰 주소(레포트 표기를 위해 main에서 주입)
        self.token_address: Optional[str] = None

    # -------------------- 홀더 수집 --------------------
    def get_top_holders_v2(self, token_addr: str, limit: int) -> List[str]:
        params = {
            "chainid": 1,
            "module": "token",
            "action": "topholders",
            "contractaddress": token_addr,
            "offset": min(limit, 1000),
            "apikey": self.api_key,
        }
        try:
            r = self.session.get(self.base_url_v2, params=params, timeout=self.config.REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            result = data.get("result", {})
            holders = result.get("holders", []) if isinstance(result, dict) else result
            if isinstance(holders, list) and holders:
                return [h.get("address", "").lower() for h in holders if h.get("address")][:limit]
        except Exception as e:
            self.logger.warning(f"v2 topholders API failed: {e}")
        return []

    def get_holders_from_tokentx_v1(self, token_addr: str, limit: int) -> List[str]:
        page = 1
        offset = min(10000, self.config.BATCH_SIZE)
        max_pages = min(20, 10000 // max(1, offset))  # 성능 제한
        addrs: Set[str] = set()
        zero = "0x0000000000000000000000000000000000000000"
        token_addr_l = token_addr.lower()
        excluded = {zero, token_addr_l} | self.routers

        def keep(addr: str) -> bool:
            return (addr and addr.startswith("0x") and len(addr) == 42 and addr not in excluded)

        while page <= max_pages and len(addrs) < limit:
            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": token_addr,
                "page": page,
                "offset": offset,
                "sort": "desc",
                "apikey": self.api_key
            }
            try:
                r = self.session.get(self.base_url, params=params, timeout=self.config.REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()
                if data.get("status") == "0" and "No transactions found" in data.get("message", ""):
                    break
                rows = data.get("result", []) or []
                if not rows:
                    break

                new_addrs = {
                    addr for t in rows
                    for addr in [(t.get("from") or "").lower(), (t.get("to") or "").lower()]
                    if keep(addr) and addr not in addrs
                }
                addrs.update(new_addrs)

                if len(rows) < offset:
                    break
                page += 1

                # 레이트리밋 지터
                time.sleep(self.config.REQUEST_DELAY * random.uniform(0.9, 1.3))

                if len(addrs) >= limit:
                    break

            except Exception as e:
                self.logger.warning(f"Error fetching tokentx page {page}: {e}")
                break

        res = list(addrs)[:limit]
        self.logger.info(f"Collected {len(res)} holder candidates from tokentx (v1)")
        return res

    def get_holders(self, token_addr: str, limit: int) -> List[str]:
        addrs = []
        try:
            addrs = self.get_top_holders_v2(token_addr, limit)
            if len(addrs) >= min(limit // 2, 100):
                self.logger.info(f"Fetched {len(addrs)} top holders via v2")
                if len(addrs) < limit:
                    self.logger.info(f"v2에서 {len(addrs)}개만 수집, v1으로 추가 수집 시도")
                    additional = self.get_holders_from_tokentx_v1(token_addr, limit - len(addrs))
                    addrs.extend([a for a in additional if a not in addrs])
                return addrs[:limit]
            self.logger.warning("v2 topholders insufficient, falling back to v1 tokentx")
        except Exception as e:
            self.logger.warning(f"v2 topholders failed ({e}), falling back to v1 tokentx")
        return self.get_holders_from_tokentx_v1(token_addr, limit)

    # -------------------- 트랜잭션 수집/정규화 --------------------
    def _validate_api_response(self, data: Dict) -> bool:
        if not isinstance(data, dict):
            return False
        if data.get("message") == "NOTOK":
            result = str(data.get("result", ""))
            if "rate limit" in result.lower():
                self.logger.warning("Rate limit hit, waiting...")
                time.sleep(2.0)
                return False
            if "no transactions found" in result.lower():
                return True
            self.logger.warning(f"API returned NOTOK: {result}")
            return False
        return True

    def get_txlist(self, address: str, startblock: int = 0, endblock: int = 99999999, sort: str = "asc") -> List[Dict]:
        self.logger.info(f"Fetching transactions for address: {address}")
        page, offset = 1, self.config.BATCH_SIZE
        max_pages = min(100, 10000 // max(1, offset))
        all_rows: List[Dict] = []

        while True:
            if page > max_pages:
                self.logger.warning(f"Reached Etherscan window limit ({max_pages} pages) for {address}")
                break

            params = {
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": startblock,
                "endblock": endblock,
                "page": page,
                "offset": offset,
                "sort": sort,
                "apikey": self.api_key
            }

            for attempt in range(self.config.MAX_RETRIES):
                try:
                    response = self.session.get(self.base_url, params=params, timeout=self.config.REQUEST_TIMEOUT)
                    response.raise_for_status()
                    data = response.json()
                    if not self._validate_api_response(data):
                        if attempt == self.config.MAX_RETRIES - 1:
                            self.logger.error(f"Failed after {self.config.MAX_RETRIES} attempts for {address}")
                            return all_rows
                        time.sleep(2 ** attempt)
                        continue

                    if data.get("status") == "0" and "No transactions found" in data.get("message", ""):
                        self.logger.info(f"No transactions found for {address}")
                        return all_rows

                    batch = data.get("result", []) or []
                    if not isinstance(batch, list) or not batch:
                        return all_rows

                    all_rows.extend(batch)
                    self.logger.info(f"Fetched {len(batch)} transactions (page {page}) for {address}")

                    if len(batch) < offset:
                        return all_rows

                    page += 1
                    time.sleep(max(self.config.REQUEST_DELAY, 0.25) * random.uniform(0.9, 1.3))
                    break

                except requests.RequestException as e:
                    self.logger.warning(f"Request attempt {attempt + 1} failed for {address}: {e}")
                    if attempt == self.config.MAX_RETRIES - 1:
                        self.logger.error(f"All retry attempts failed for {address}")
                        return all_rows
                    time.sleep(2 ** attempt)
            else:
                break

        self.logger.info(f"Total {len(all_rows)} transactions fetched for {address}")
        return all_rows

    def _is_sell(self, method_name: str) -> bool:
        """매도 거래 추정 (보수적으로 확대)"""
        if not method_name:
            return False
        n = method_name.lower()
        # 정확한 방향성은 로그/경로 필요. 여기서는 recall을 높이는 타협안.
        return any(x in n for x in [
            "tokensforeth",     # v2 패턴
            "exactinputsingle", # v3 추정
            "exactinput"        # v3 추정
        ])

    def normalize_tx_rows(self, rows: List[Dict], holder: str) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame()

        processed_rows = []
        for tx in rows:
            try:
                if not all(k in tx for k in ["hash", "timeStamp", "blockNumber", "to"]):
                    continue

                to_addr = (tx.get("to") or "").lower()
                if to_addr not in self.routers:
                    continue  # 라우터 호출만 분석

                ts = int(tx["timeStamp"])
                time_utc = datetime.fromtimestamp(ts, UTC)
                if not within_window(time_utc, self.window_start_utc, self.window_end_utc):
                    continue

                input_data = tx.get("input", "") or ""
                method_sig = input_data[:10].lower() if (input_data.startswith("0x") and len(input_data) >= 10) else ""
                method_name = self.sig_map.get(method_sig, f"sig:{method_sig}" if method_sig else "unknown")

                is_error = str(tx.get("isError", "0")) == "1"
                receipt_failed = str(tx.get("txreceipt_status", "1")) == "0"
                status = "fail" if (is_error or receipt_failed) else "success"

                block_num = int(tx["blockNumber"])
                gas_used = int(tx.get("gasUsed", 0))
                gas_price = int(tx.get("gasPrice", 0))
                value_eth = float(tx.get("value", 0)) / 1e18
                gas_fee_eth = (gas_used * gas_price) / 1e18 if gas_used and gas_price else 0.0

                processed_rows.append({
                    "holder": holder.lower(),
                    "hash": tx["hash"],
                    "time_utc": time_utc,
                    "block": block_num,
                    "to": to_addr,
                    "method_guess": method_name,
                    "is_sell_path": self._is_sell(method_name),
                    "value_eth": value_eth,
                    "gas_used": gas_used,
                    "gas_price_gwei": gas_price / 1e9 if gas_price else 0.0,
                    "gas_fee_eth": gas_fee_eth,
                    "isError": tx.get("isError", "0"),
                    "txreceipt_status": tx.get("txreceipt_status", "1"),
                    "status": status,
                })
            except Exception as e:
                self.logger.warning(f"Error processing tx {tx.get('hash', '')}: {e}")
                continue

        df = pd.DataFrame(processed_rows)
        if not df.empty:
            df = df.drop_duplicates("hash", keep="first")
        self.logger.info(f"Processed {len(df)} router transactions in window for {holder}")
        return df

    # -------------------- 분석 --------------------
    def _enrich_gas_with_receipts(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        target = df[df["status"] == "fail"].sort_values("time_utc", ascending=False).head(self.config.ENRICH_RECEIPT_TOPN)
        for h in target["hash"].tolist():
            try:
                r = self.session.get(self.base_url, params={
                    "module": "proxy",
                    "action": "eth_getTransactionReceipt",
                    "txhash": h,
                    "apikey": self.api_key
                }, timeout=self.config.REQUEST_TIMEOUT)
                js = r.json()
                result = js.get("result") or {}
                eff = result.get("effectiveGasPrice")
                gas_used_hex = result.get("gasUsed")
                if eff and gas_used_hex:
                    eff_int = int(eff, 16)
                    used = int(gas_used_hex, 16)
                    df.loc[df["hash"] == h, "gas_price_gwei"] = eff_int / 1e9
                    df.loc[df["hash"] == h, "gas_fee_eth"] = (eff_int * used) / 1e18
                time.sleep(0.1)
            except Exception:
                pass

    def cluster_failures(self, df: pd.DataFrame, window_minutes: Optional[int] = None) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["holder", "start", "end", "count", "gas_waste_eth"])
        window_minutes = window_minutes or self.config.CLUSTER_WINDOW_MINUTES
        fails = df[df["status"] == "fail"].sort_values("time_utc").copy()
        if fails.empty:
            return pd.DataFrame(columns=["holder", "start", "end", "count", "gas_waste_eth"])

        clusters = []
        i = 0
        while i < len(fails):
            start_time = fails.iloc[i]["time_utc"]
            start_idx = i
            while i + 1 < len(fails):
                next_time = fails.iloc[i + 1]["time_utc"]
                if (next_time - start_time).total_seconds() <= window_minutes * 60:
                    i += 1
                else:
                    break
            end_idx = i
            count = end_idx - start_idx + 1
            if count >= self.config.MIN_CLUSTER_SIZE:
                cluster_txs = fails.iloc[start_idx:end_idx + 1]
                gas_waste = cluster_txs["gas_fee_eth"].sum()
                clusters.append({
                    "holder": fails.iloc[start_idx]["holder"],
                    "start": start_time,
                    "end": fails.iloc[end_idx]["time_utc"],
                    "count": count,
                    "gas_waste_eth": gas_waste
                })
            i += 1
        return pd.DataFrame(clusters)

    def detect_honeypot_indicators(self, summary_df: pd.DataFrame) -> Dict[str, List[str]]:
        indicators = {}
        for _, row in summary_df.iterrows():
            holder = row['holder']
            holder_indicators = []

            if row['fail_rate'] >= self.config.HIGH_FAIL_RATE_THRESHOLD:
                holder_indicators.append(f"높은 실패율: {row['fail_rate']:.1%}")
            if row['max_consecutive_fails'] >= self.config.CONSECUTIVE_FAIL_THRESHOLD:
                holder_indicators.append(f"연속 실패: {row['max_consecutive_fails']}회")
            if row['clusters_2h_count'] >= 3:
                holder_indicators.append(f"실패 클러스터: {row['clusters_2h_count']}개")
            if row['total_gas_waste_eth'] > 0.05:
                holder_indicators.append(f"가스 낭비: {row['total_gas_waste_eth']:.3f} ETH")
            if 'sell_fail_rate' in row and row['sell_txs'] >= 3 and row['sell_fail_rate'] >= 0.7:
                holder_indicators.append(f"매도 실패율 높음: {row['sell_fail_rate']:.1%} / {row['sell_txs']}건")

            if holder_indicators:
                indicators[holder] = holder_indicators
        return indicators

    def analyze_holder(self, address: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        self.logger.info(f"Starting analysis for holder: {address}")
        raw_txs = self.get_txlist(address)
        df = self.normalize_tx_rows(raw_txs, address)

        if df.empty:
            empty_summary = pd.DataFrame([{
                "holder": address.lower(),
                "tx_to_router": 0,
                "fails": 0,
                "success": 0,
                "fail_rate": 0.0,
                "max_consecutive_fails": 0,
                "avg_gas_failed": 0.0,
                "total_gas_waste_eth": 0.0,
                "clusters_2h_count": 0,
                "sell_txs": 0,
                "sell_fail_rate": 0.0,
                "first_tx": None,
                "last_tx": None
            }])
            return df, empty_summary, pd.DataFrame()

        if self.config.USE_RECEIPT_GAS:
            self._enrich_gas_with_receipts(df)

        df = df.sort_values("time_utc").reset_index(drop=True)

        total_txs = len(df)
        failed_txs = int((df["status"] == "fail").sum())
        success_txs = total_txs - failed_txs
        fail_rate = failed_txs / total_txs if total_txs else 0.0

        # 연속 실패 계산
        consecutive_fails = 0
        max_consecutive_fails = 0
        for st in df["status"]:
            if st == "fail":
                consecutive_fails += 1
                max_consecutive_fails = max(max_consecutive_fails, consecutive_fails)
            else:
                consecutive_fails = 0

        # 가스비 분석
        failed_df = df[df["status"] == "fail"]
        avg_gas_failed = float(failed_df["gas_used"].mean()) if not failed_df.empty else 0.0
        total_gas_waste = float(failed_df["gas_fee_eth"].sum()) if not failed_df.empty else 0.0

        # 매도 거래 분석
        sell_df = df[df["is_sell_path"] == True]
        sell_fail_rate = float((sell_df["status"] == "fail").mean()) if not sell_df.empty else 0.0

        # 실패 클러스터 분석
        clusters = self.cluster_failures(df)

        summary = pd.DataFrame([{
            "holder": address.lower(),
            "tx_to_router": total_txs,
            "fails": failed_txs,
            "success": success_txs,
            "fail_rate": round(fail_rate, 4),
            "max_consecutive_fails": int(max_consecutive_fails),
            "avg_gas_failed": round(avg_gas_failed, 0),
            "total_gas_waste_eth": round(total_gas_waste, 6),
            "clusters_2h_count": len(clusters),
            "sell_txs": int(len(sell_df)),
            "sell_fail_rate": round(sell_fail_rate, 4),
            "first_tx": df["time_utc"].min(),
            "last_tx": df["time_utc"].max()
        }])

        self.logger.info(f"Analysis complete for {address}: {total_txs} txs (window), {fail_rate:.1%} fail rate")
        return df, summary, clusters

    def safe_concat_dataframes(self, df_list: List[pd.DataFrame], name: str) -> pd.DataFrame:
        if not df_list:
            self.logger.warning(f"No dataframes to concat for {name}")
            return pd.DataFrame()
        try:
            result = pd.concat(df_list, ignore_index=True)
            self.logger.info(f"Successfully concatenated {len(df_list)} dataframes for {name}")
            return result
        except Exception as e:
            self.logger.error(f"Error concatenating dataframes for {name}: {e}")
            return pd.DataFrame()

    def analyze_multiple_holders(self, holders: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        total_holders = len(holders)
        self.logger.info(f"Starting analysis for {total_holders} token holders")

        all_transactions, all_summaries, all_clusters = [], [], []
        failed_holders = []

        for i, holder in enumerate(holders, 1):
            try:
                tx_df, summary_df, cluster_df = self.analyze_holder(holder)
                if not tx_df.empty:
                    all_transactions.append(tx_df)
                if not summary_df.empty:
                    all_summaries.append(summary_df)
                if not cluster_df.empty:
                    all_clusters.append(cluster_df)

                if i % 50 == 0 or i == total_holders:
                    progress = (i / total_holders) * 100
                    self.logger.info(f"Progress: {i}/{total_holders} ({progress:.1f}%) completed")
                    print(f"진행률: {progress:.1f}% ({i}/{total_holders})")
            except Exception as e:
                failed_holders.append(holder)
                self.logger.error(f"Error analyzing holder {holder}: {e}")
                continue

        if failed_holders:
            self.logger.warning(f"Failed to analyze {len(failed_holders)} holders")

        combined_txs = self.safe_concat_dataframes(all_transactions, "transactions")
        combined_summaries = self.safe_concat_dataframes(all_summaries, "summaries")
        combined_clusters = self.safe_concat_dataframes(all_clusters, "clusters")
        return combined_txs, combined_summaries, combined_clusters

    # -------------------- 출력/저장 --------------------
    def generate_report(self, tx_df: pd.DataFrame, summary_df: pd.DataFrame, cluster_df: pd.DataFrame, token_name: str) -> str:
        report = []
        report.append("=" * 80)
        report.append(f"{token_name.upper()} 토큰 허니팟 분석 보고서")
        if hasattr(self, 'token_address') and self.token_address:
            report.append(f"토큰 주소: {self.token_address}")
        report.append("=" * 80)

        report.append(f"분석 시간: {datetime.now(UTC).astimezone(KST).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        if self.window_start_utc or self.window_end_utc:
            s = self.window_start_utc.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S %Z') if self.window_start_utc else "-"
            e = self.window_end_utc.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S %Z') if self.window_end_utc else "-"
            report.append(f"적용 윈도우: {s} ~ {e}")
        else:
            report.append("적용 윈도우: 전체 기간")
        report.append("")

        if summary_df.empty:
            report.append("분석할 데이터가 없습니다.")
            return "\n".join(report)

        total_holders = len(summary_df)
        holders_with_txs = len(summary_df[summary_df["tx_to_router"] > 0])
        total_txs = int(summary_df["tx_to_router"].sum())
        total_fails = int(summary_df["fails"].sum())
        overall_fail_rate = (total_fails / total_txs) if total_txs > 0 else 0.0
        total_gas_waste = float(summary_df["total_gas_waste_eth"].sum())

        report.append("📊 전체 통계:")
        report.append(f"  - 분석 홀더 수: {total_holders}")
        report.append(f"  - 거래 활동 홀더: {holders_with_txs}")
        report.append(f"  - 총 라우터 거래(윈도우 내): {total_txs:,}")
        report.append(f"  - 실패 거래: {total_fails:,}")
        report.append(f"  - 전체 실패율: {overall_fail_rate:.1%}")
        report.append(f"  - 총 가스 낭비: {total_gas_waste:.4f} ETH")
        report.append("")

        indicators = self.detect_honeypot_indicators(summary_df)
        high_risk_holders = len([h for h, inds in indicators.items() if len(inds) >= 2])

        if indicators:
            report.append("🚨 허니팟 지표 탐지:")
            report.append(f"  - 지표 보유 홀더: {len(indicators)}명")
            report.append(f"  - 고위험 홀더 (2개 이상 지표): {high_risk_holders}명")
            report.append("")
            sorted_indicators = sorted(indicators.items(), key=lambda x: len(x[1]), reverse=True)
            for i, (holder, holder_indicators) in enumerate(sorted_indicators[:10], 1):
                report.append(f"  [{i}] {holder[:10]}...:")
                for indicator in holder_indicators:
                    report.append(f"    ⚠️  {indicator}")
            if len(indicators) > 10:
                report.append(f"  ... 및 {len(indicators) - 10}명 추가")
            report.append("")

        # 매도 실패 분석(요약)
        high_sell_fail = pd.DataFrame()
        sell_analysis = summary_df[summary_df["sell_txs"] >= 3]
        if not sell_analysis.empty:
            high_sell_fail = sell_analysis[sell_analysis["sell_fail_rate"] >= 0.7]
            report.append("🔴 매도 실패 분석:")
            report.append(f"  - 매도 시도 홀더 (3회 이상): {len(sell_analysis)}명")
            report.append(f"  - 매도 실패율 70% 이상: {len(high_sell_fail)}명")
            if not high_sell_fail.empty:
                avg_sell_fail_rate = high_sell_fail["sell_fail_rate"].mean()
                report.append(f"  - 평균 매도 실패율: {avg_sell_fail_rate:.1%}")
            report.append("")

        # 클러스터 분석
        if not cluster_df.empty:
            total_clusters = len(cluster_df)
            avg_cluster_size = cluster_df["count"].mean()
            total_cluster_waste = cluster_df["gas_waste_eth"].sum()
            report.append("📈 실패 클러스터 분석:")
            report.append(f"  - 총 클러스터: {total_clusters}개")
            report.append(f"  - 평균 클러스터 크기: {avg_cluster_size:.1f}회")
            report.append(f"  - 클러스터 가스 낭비: {total_cluster_waste:.4f} ETH")
            report.append("")

        # 홀더별 분석(매도 실패자 요약)
        report.append("👥 홀더별 분석 결과:")
        sell_fail_holders = summary_df[(summary_df["sell_txs"] >= 1) & (summary_df["sell_fail_rate"] > 0)]\
            .sort_values(["sell_fail_rate", "total_gas_waste_eth"], ascending=False)
        if not sell_fail_holders.empty:
            report.append(f"  📊 매도 시도 실패 홀더: {len(sell_fail_holders)}명")
            report.append("")
            for idx, (_, row) in enumerate(sell_fail_holders.head(50).iterrows(), 1):
                holder_short = row['holder'][:12] + "..."
                report.append(f"  [{idx}] {holder_short}:")
                report.append(
                    f"    전체거래: {int(row['tx_to_router'])} | 전체실패율: {row['fail_rate']:.1%} | "
                    f"매도실패율: {row['sell_fail_rate']:.1%} ({int(row['sell_txs'])}건 중)"
                )
                report.append(
                    f"    연속실패: {int(row['max_consecutive_fails'])}회 | "
                    f"가스낭비: {row['total_gas_waste_eth']:.4f} ETH | 클러스터: {int(row['clusters_2h_count'])}개"
                )
        else:
            report.append("  매도 시도 실패 홀더가 없습니다.")
        report.append("")

        # 결론(간단 판정)
        report.append(f"📋 {token_name.upper()} 토큰 분석 결론:")
        if overall_fail_rate >= 0.20:
            report.append("  🔴 HIGH RISK: 높은 전체 실패율")
        elif overall_fail_rate >= 0.10:
            report.append("  🟡 MEDIUM RISK: 중간 수준 실패율")
        else:
            report.append("  🟢 LOW RISK: 낮은 실패율")

        if high_risk_holders >= 50:
            report.append("  🔴 HIGH RISK: 매우 다수 홀더에서 허니팟 지표 발견")
        elif high_risk_holders >= 20:
            report.append("  🟡 MEDIUM RISK: 다수 홀더에서 허니팟 지표 발견")
        elif high_risk_holders >= 5:
            report.append("  🟡 MEDIUM RISK: 일부 홀더에서 허니팟 지표 발견")
        else:
            report.append("  🟢 LOW RISK: 허니팟 지표 보유 홀더 적음")

        num_high_sell_fail = len(high_sell_fail) if not high_sell_fail.empty else 0
        if not sell_analysis.empty and num_high_sell_fail >= 10:
            report.append("  🔴 HIGH RISK: 다수 홀더의 매도 시도 실패")
        elif not sell_analysis.empty and num_high_sell_fail >= 3:
            report.append("  🟡 MEDIUM RISK: 일부 홀더의 매도 시도 실패")

        risk_factors = 0
        if overall_fail_rate >= 0.10: risk_factors += 1
        if high_risk_holders >= 20: risk_factors += 1
        if num_high_sell_fail >= 3: risk_factors += 1
        if total_gas_waste >= 50.0: risk_factors += 1
        if len(indicators) >= 50: risk_factors += 1

        report.append("")
        if risk_factors >= 4:
            report.append(f"  🚨 최종 결론: {token_name.upper()}는 허니팟 스캠 토큰으로 강력히 의심됨")
        elif risk_factors >= 3:
            report.append(f"  🔴 최종 결론: {token_name.upper()}는 허니팟 가능성이 매우 높은 위험한 토큰")
        elif risk_factors >= 2:
            report.append(f"  🟡 최종 결론: {token_name.upper()}는 허니팟 가능성이 있는 위험한 토큰")
        else:
            report.append("  🟢 최종 결론: 일부 지표 존재하나 결정적 증거 부족")

        report.append("")
        report.append(f"  📊 위험 요소 점수: {risk_factors}/5")
        report.append(f"  📊 지표 보유 홀더 비율: {len(indicators)}/{total_holders} ({len(indicators)/total_holders*100:.1f}%)")
        report.append(f"  📊 고위험 홀더 비율: {high_risk_holders}/{total_holders} ({(high_risk_holders/total_holders*100 if total_holders else 0):.1f}%)")

        report.append("=" * 80)
        return "\n".join(report)

    def save_results(self, tx_df: pd.DataFrame, summary_df: pd.DataFrame, cluster_df: pd.DataFrame, token_name: str) -> Dict[str, str]:
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        holder_count = len(summary_df) if not summary_df.empty else 0
        token_lower = token_name.lower()

        filenames = {
            "transactions": f"{token_lower}_transactions_{holder_count}_{timestamp}.csv",
            "summary": f"{token_lower}_summary_{holder_count}_{timestamp}.csv",
            "clusters": f"{token_lower}_clusters_{holder_count}_{timestamp}.csv",
            "report": f"{token_lower}_report_{holder_count}_{timestamp}.txt"
        }
        saved = {}
        try:
            if not tx_df.empty:
                tx_df.to_csv(filenames["transactions"], index=False, encoding='utf-8-sig')
                self.logger.info(f"Saved transactions to {filenames['transactions']}")
                saved["transactions"] = filenames["transactions"]

            if not summary_df.empty:
                summary_df.to_csv(filenames["summary"], index=False, encoding='utf-8-sig')
                self.logger.info(f"Saved summary to {filenames['summary']}")
                saved["summary"] = filenames["summary"]

            if not cluster_df.empty:
                cluster_df.to_csv(filenames["clusters"], index=False, encoding='utf-8-sig')
                self.logger.info(f"Saved clusters to {filenames['clusters']}")
                saved["clusters"] = filenames["clusters"]

            report = self.generate_report(tx_df, summary_df, cluster_df, token_name)
            with open(filenames["report"], 'w', encoding='utf-8') as f:
                f.write(report)
            self.logger.info(f"Saved report to {filenames['report']}")
            saved["report"] = filenames["report"]
            return saved

        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            return saved

# -------------------- 메인 --------------------
def main():
    # === 환경변수 필수 ===
    api_key = os.getenv("ETHERSCAN_API_KEY", "").strip()
    token_address = os.getenv("TOKEN_ADDRESS", "").strip().lower()
    token_name = os.getenv("TOKEN_NAME", "").strip()
    holder_limit_env = os.getenv("HOLDER_LIMIT", "").strip()

    missing = []
    if not api_key: missing.append("ETHERSCAN_API_KEY")
    if not token_address: missing.append("TOKEN_ADDRESS")
    if not token_name: missing.append("TOKEN_NAME")
    if not holder_limit_env: missing.append("HOLDER_LIMIT")
    if missing:
        print("\n❌ 필수 환경변수가 누락되었습니다:")
        for m in missing:
            print(f"  - {m}")
        print("\nPowerShell 예시:")
        print('$env:ETHERSCAN_API_KEY = "실제_API_키"')
        print('$env:TOKEN_ADDRESS = "토큰_주소"')
        print('$env:TOKEN_NAME = "토큰이름"')
        print('$env:HOLDER_LIMIT = "홀더수"')
        sys.exit(1)

    try:
        holder_limit = int(holder_limit_env)
    except ValueError:
        print("❌ HOLDER_LIMIT 는 정수여야 합니다. 예: 335")
        sys.exit(1)

    logger = setup_logging(token_name)

    config = Config()
    config.HOLDER_LIMIT = holder_limit
    # 성능/정확도 트레이드오프 조정 가능하면 여기서 환경변수로 추가 세팅해도 됨.

    logger.info(f"=== {token_name} 토큰 분석 설정 ===")
    logger.info(f"토큰 이름: {token_name}")
    logger.info(f"토큰 주소: {token_address}")
    logger.info(f"목표 홀더 수: {config.HOLDER_LIMIT}개")
    logger.info(f"성능 최적화 설정:")
    logger.info(f"  - 요청 지연: {config.REQUEST_DELAY}초 (지터 적용)")
    logger.info(f"  - 타임아웃: {config.REQUEST_TIMEOUT}초")
    logger.info(f"  - Receipt 보정: {config.USE_RECEIPT_GAS} (실패 Top-N {config.ENRICH_RECEIPT_TOPN})")

    try:
        analyzer = EtherscanAnalyzer(api_key=api_key, config=config, logger=logger)
        analyzer.token_address = token_address  # 보고서 표기용
    except ValueError as e:
        logger.error(f"초기화 실패: {e}")
        print(f"\n❌ 에러: {e}")
        print('PowerShell에서 다음 명령어로 설정하세요:')
        print('$env:ETHERSCAN_API_KEY = "실제_API_키"')
        print('$env:TOKEN_ADDRESS = "토큰_주소"')
        print('$env:TOKEN_NAME = "토큰이름"')
        print('$env:HOLDER_LIMIT = "홀더수"')
        sys.exit(1)

    print(f"🔍 {token_name} 토큰의 상위 {config.HOLDER_LIMIT}개 홀더 수집 중...")
    holders = analyzer.get_holders(token_address, limit=config.HOLDER_LIMIT)
    if not holders:
        logger.error("홀더 목록을 가져오지 못했습니다.")
        print("\n❌ 홀더 목록 수집 실패. 다음 사항을 확인하세요:")
        print("1. ETHERSCAN_API_KEY가 올바른지")
        print("2. TOKEN_ADDRESS가 유효한지")
        print("3. API 쿼터가 남아있는지")
        sys.exit(1)

    logger.info(f"Successfully fetched {len(holders)} {token_name} holders")
    print(f"✅ {len(holders)}개 홀더 수집 완료")

    print(f"\n🔍 {token_name} 토큰 {len(holders)}개 홀더 분석 시작...")
    print(f"예상 소요 시간(대략): {len(holders) * config.REQUEST_DELAY / 60:.1f}분")
    print("분석 진행 중... (50개 단위로 진행률 표시)")

    start_time = time.time()
    tx_df, summary_df, cluster_df = analyzer.analyze_multiple_holders(holders)
    elapsed_minutes = (time.time() - start_time) / 60.0
    print(f"✅ 분석 완료! 실제 소요 시간: {elapsed_minutes:.1f}분")

    print("\n" + "="*60)
    print(analyzer.generate_report(tx_df, summary_df, cluster_df, token_name))

    if not summary_df.empty:
        print(f"\n=== {token_name} 상위 위험 홀더 (실패율 기준) ===")
        risky = summary_df[summary_df["fail_rate"] >= 0.3].sort_values(
            ["fail_rate", "total_gas_waste_eth"], ascending=False
        )
        if not risky.empty:
            display_cols = ["holder", "tx_to_router", "fails", "fail_rate", "sell_fail_rate", "total_gas_waste_eth", "clusters_2h_count"]
            print(risky[display_cols].head(20).to_string(index=False))
        else:
            print("실패율 30% 이상인 홀더가 없습니다.")

    if not tx_df.empty:
        print(f"\n=== {token_name} 최근 실패 거래 분석 ===")
        recent_fails = tx_df[tx_df["status"] == "fail"].sort_values("time_utc", ascending=False).head(15)
        if not recent_fails.empty:
            cols = ["holder", "time_utc", "method_guess", "gas_fee_eth", "is_sell_path"]
            print(recent_fails[cols].to_string(index=False))

    saved_files = analyzer.save_results(tx_df, summary_df, cluster_df, token_name)
    if saved_files:
        print(f"\n📁 저장된 파일들:")
        for k, v in saved_files.items():
            print(f"  - {k}: {v}")

    print(f"\n✅ {token_name} 토큰 분석 완료")
    print(f"총 분석 홀더 수: {len(holders)}개")
    print(f"총 트랜잭션 수: {len(tx_df):,}개" if not tx_df.empty else "총 트랜잭션 수: 0개")
    print(f"실제 분석 시간: {elapsed_minutes:.1f}분")

if __name__ == "__main__":
    main()
