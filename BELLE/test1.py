# pip install requests pandas python-dateutil

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
from requests.adapters import HTTPAdapter, Retry

# -------------------- 로깅 --------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('belle0_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

UTC = timezone.utc
KST = timezone(timedelta(hours=9))

# -------------------- 설정 --------------------
@dataclass
class Config:
    MIN_CLUSTER_SIZE: int = 3
    CLUSTER_WINDOW_MINUTES: int = 120
    HIGH_FAIL_RATE_THRESHOLD: float = 0.8
    CONSECUTIVE_FAIL_THRESHOLD: int = 10
    MAX_RETRIES: int = 3
    BATCH_SIZE: int = 1000          # txlist offset (<=1000 권장)
    REQUEST_DELAY: float = 0.30     # 5 rps 여유
    REQUEST_TIMEOUT: int = 30
    HOLDER_LIMIT: int = 557         # 홀더 개수를 557개로 변경
    USE_RECEIPT_GAS: bool = False   # True면 일부 Tx receipt로 보정
    ENRICH_RECEIPT_TOPN: int = 200  # receipt 보정 실패 Tx 최대 수

# -------------------- 유틸 --------------------
def parse_window_from_env() -> Tuple[Optional[datetime], Optional[datetime], str]:
    """
    시간 윈도우를 환경변수에서 읽어 UTC로 반환.
    우선순위:
      1) WINDOW_START_KST, WINDOW_END_KST  (KST 로 해석)
      2) WINDOW_START_UTC, WINDOW_END_UTC  (UTC 로 해석)
      3) 없음 -> (None, None)
    포맷: YYYY-MM-DD HH:MM:SS
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
            logger.warning(f"Failed to parse KST window: {e}")

    if us and ue:
        try:
            start_utc = datetime.strptime(us, fmt).replace(tzinfo=UTC)
            end_utc   = datetime.strptime(ue, fmt).replace(tzinfo=UTC)
            return start_utc, end_utc, "UTC"
        except Exception as e:
            logger.warning(f"Failed to parse UTC window: {e}")

    return None, None, ""

def within_window(ts_utc: datetime, start_utc: Optional[datetime], end_utc: Optional[datetime]) -> bool:
    if start_utc and ts_utc < start_utc:
        return False
    if end_utc and ts_utc > end_utc:
        return False
    return True

# -------------------- 메인 클래스 --------------------
class EtherscanAnalyzer:
    def __init__(self, api_key: str, config: Config = None):
        self.api_key = api_key or os.getenv("ETHERSCAN_API_KEY")
        if not self.api_key or self.api_key == "YOUR_API_KEY":
            raise ValueError("ETHERSCAN_API_KEY 환경변수가 설정되지 않았습니다.")
        self.config = config or Config()
        self.base_url = "https://api.etherscan.io/api"         # v1
        self.base_url_v2 = "https://api.etherscan.io/v2/api"   # v2 (beta)

        # 세션 + Retry
        self.session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        # 시간 윈도우(UTC) 설정
        self.window_start_utc, self.window_end_utc, self.window_kind = parse_window_from_env()
        if self.window_start_utc or self.window_end_utc:
            s = self.window_start_utc.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S %Z") if self.window_start_utc else "-"
            e = self.window_end_utc.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S %Z") if self.window_end_utc else "-"
            logger.info(f"Applied time window [{self.window_kind}]: {s} ~ {e}")

        # 라우터 주소들(필요시 커스텀 라우터 수동 추가)
        self.routers: Set[str] = {
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2
            "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3
            "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3 Router 2
            "0x1111111254fb6c44bac0bed2854e76f90643097d",  # 1inch v4
            "0x1111111254eeb25477b68fb85ed929f73a960582",  # 1inch v5
            "0xdef1c0ded9bec7f1a1670819833240f027b25eff",  # 0x
            "0x881d40237659c251811cec9c364ef91dc08d300c",  # Metamask
        }
        # 예) 의심 라우터가 있다면 여기에 직접 추가
        suspect_router = os.getenv("EXTRA_ROUTER", "").strip().lower()
        if suspect_router.startswith("0x") and len(suspect_router) == 42:
            self.routers.add(suspect_router)
            logger.info(f"Added EXTRA_ROUTER: {suspect_router}")

        # 함수 시그니처 매핑
        self.sig_map: Dict[str, str] = {
            # Uniswap V2
            "0x18cbafe5": "swapExactTokensForETH",                                   # SELL
            "0x791ac947": "swapExactTokensForETHSupportingFeeOnTransferTokens",      # SELL
            "0x4a25d94a": "swapTokensForExactETH",                                   # SELL
            "0x7ff36ab5": "swapExactETHForTokens",                                   # BUY
            "0xfb3bdb41": "swapETHForExactTokens",                                   # BUY
            "0x38ed1739": "swapExactTokensForTokens",
            "0x8803dbee": "swapTokensForExactTokens",
            "0xb6f9de95": "swapExactETHForTokensSupportingFeeOnTransferTokens",      # BUY
            # 1inch
            "0x12aa3caf": "swap",        # v5
            "0x0502b1c5": "unoswap",
            "0x2e95b6c8": "unoswap",
        }

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
        r = self.session.get(self.base_url_v2, params=params, timeout=self.config.REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        result = data.get("result", {})
        holders = result.get("holders", [])
        if isinstance(holders, list) and holders:
            return [h.get("address", "").lower() for h in holders if h.get("address")][:limit]
        if isinstance(result, list) and result:
            return [h.get("address", "").lower() for h in result if h.get("address")][:limit]
        return []

    def get_holders_from_tokentx_v1(self, token_addr: str, limit: int) -> List[str]:
        page = 1
        offset = min(10000, self.config.BATCH_SIZE)
        max_pages = min(100, 10000 // max(1, offset))
        addrs: Set[str] = set()
        zero = "0x0000000000000000000000000000000000000000"
        token_addr_l = token_addr.lower()

        while page <= max_pages:
            params = {
                "module": "account",
                "action": "tokentx",
                "contractaddress": token_addr,
                "page": page,
                "offset": offset,
                "sort": "asc",
                "apikey": self.api_key
            }
            r = self.session.get(self.base_url, params=params, timeout=self.config.REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if data.get("status") == "0" and "No transactions found" in data.get("message", ""):
                break
            rows = data.get("result", []) or []
            if not rows:
                break

            for t in rows:
                frm = (t.get("from") or "").lower()
                to = (t.get("to") or "").lower()
                if frm and frm != zero and frm != token_addr_l:
                    addrs.add(frm)
                if to and to != zero and to != token_addr_l:
                    addrs.add(to)

            if len(rows) < offset:
                break
            page += 1
            time.sleep(self.config.REQUEST_DELAY)
            if len(addrs) >= limit:
                break

        res = list(addrs)[:limit]
        logger.info(f"Collected {len(res)} holder candidates from tokentx (v1)")
        return res

    def get_holders(self, token_addr: str, limit: int) -> List[str]:
        try:
            addrs = self.get_top_holders_v2(token_addr, limit)
            if addrs:
                logger.info(f"Fetched {len(addrs)} top holders via v2")
                return addrs
            logger.warning("v2 topholders returned empty, falling back to v1 tokentx")
        except Exception as e:
            logger.warning(f"v2 topholders failed ({e}), falling back to v1 tokentx")
        return self.get_holders_from_tokentx_v1(token_addr, limit)

    # -------------------- 트랜잭션 수집/정규화 --------------------
    def _validate_api_response(self, data: Dict) -> bool:
        if not isinstance(data, dict):
            return False
        if data.get("message") == "NOTOK":
            result = str(data.get("result", ""))
            if "rate limit" in result.lower():
                logger.warning("Rate limit hit, waiting...")
                time.sleep(1)
                return False
            logger.warning(f"API returned NOTOK: {result}")
            return False
        return True

    def get_txlist(self, address: str, startblock: int = 0, endblock: int = 99999999, sort: str = "asc") -> List[Dict]:
        logger.info(f"Fetching transactions for address: {address}")
        page, offset = 1, self.config.BATCH_SIZE
        max_pages = min(100, 10000 // max(1, offset))
        all_rows: List[Dict] = []

        while True:
            if page > max_pages:
                logger.warning(f"Reached Etherscan window limit ({max_pages} pages) for {address}")
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
                            logger.error(f"Failed after {self.config.MAX_RETRIES} attempts for {address}")
                            return all_rows
                        time.sleep(2 ** attempt)
                        continue

                    if data.get("status") == "0" and "No transactions found" in data.get("message", ""):
                        logger.info(f"No transactions found for {address}")
                        return all_rows

                    batch = data.get("result", []) or []
                    if not isinstance(batch, list):
                        return all_rows
                    if not batch:
                        return all_rows

                    all_rows.extend(batch)
                    logger.info(f"Fetched {len(batch)} transactions (page {page}) for {address}")

                    if len(batch) < offset:
                        return all_rows

                    page += 1
                    time.sleep(max(self.config.REQUEST_DELAY, 0.25))
                    break

                except requests.RequestException as e:
                    logger.warning(f"Request attempt {attempt + 1} failed for {address}: {e}")
                    if attempt == self.config.MAX_RETRIES - 1:
                        logger.error(f"All retry attempts failed for {address}")
                        return all_rows
                    time.sleep(2 ** attempt)
            else:
                break

        logger.info(f"Total {len(all_rows)} transactions fetched for {address}")
        return all_rows

    def _is_sell(self, method_name: str) -> bool:
        if not method_name:
            return False
        name = method_name.lower()
        return ("tokensforeth" in name)  # swapExactTokensForETH / swapTokensForExactETH 등

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
                    continue  # 라우터 호출만

                # 시간 파싱 (UTC aware) + 윈도우 필터
                ts = int(tx["timeStamp"])
                time_utc = datetime.fromtimestamp(ts, UTC)
                if not within_window(time_utc, self.window_start_utc, self.window_end_utc):
                    continue  # <<<<<<<<<<<< 시간 윈도우 적용 핵심

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
                gas_fee_eth = (gas_used * gas_price) / 1e18

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
                logger.warning(f"Error processing tx {tx.get('hash', '')}: {e}")
                continue

        df = pd.DataFrame(processed_rows)
        if not df.empty:
            df = df.drop_duplicates("hash", keep="first")
        logger.info(f"Processed {len(df)} router transactions in window for {holder}")
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
            if row['total_gas_waste_eth'] > 0.1:
                holder_indicators.append(f"가스 낭비: {row['total_gas_waste_eth']:.3f} ETH")
            if 'sell_fail_rate' in row and row['sell_txs'] >= 5 and row['sell_fail_rate'] >= 0.5:
                holder_indicators.append(f"매도 실패율 높음(전용): {row['sell_fail_rate']:.1%} / {row['sell_txs']}건")
            if holder_indicators:
                indicators[holder] = holder_indicators
        return indicators

    def analyze_holder(self, address: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logger.info(f"Starting analysis for holder: {address}")
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

        # 연속 실패
        consecutive_fails = 0
        max_consecutive_fails = 0
        for status in df["status"]:
            if status == "fail":
                consecutive_fails += 1
                max_consecutive_fails = max(max_consecutive_fails, consecutive_fails)
            else:
                consecutive_fails = 0

        # 가스
        failed_df = df[df["status"] == "fail"]
        avg_gas_failed = float(failed_df["gas_used"].mean()) if not failed_df.empty else 0.0
        total_gas_waste = float(failed_df["gas_fee_eth"].sum()) if not failed_df.empty else 0.0

        # 매도 경로 전용 (보조)
        sell_df = df[df["is_sell_path"] == True]
        sell_fail_rate = float((sell_df["status"] == "fail").mean()) if not sell_df.empty else 0.0

        # 클러스터
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

        logger.info(f"Analysis complete for {address}: {total_txs} txs (window), {fail_rate:.1%} fail rate")
        return df, summary, clusters

    def safe_concat_dataframes(self, df_list: List[pd.DataFrame], name: str) -> pd.DataFrame:
        if not df_list:
            logger.warning(f"No dataframes to concat for {name}")
            return pd.DataFrame()
        try:
            result = pd.concat(df_list, ignore_index=True)
            logger.info(f"Successfully concatenated {len(df_list)} dataframes for {name}")
            return result
        except Exception as e:
            logger.error(f"Error concatenating dataframes for {name}: {e}")
            return pd.DataFrame()

    def analyze_multiple_holders(self, holders: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logger.info(f"Starting analysis for {len(holders)} holders")
        all_transactions, all_summaries, all_clusters = [], [], []

        for i, holder in enumerate(holders, 1):
            logger.info(f"Processing holder {i}/{len(holders)}: {holder}")
            try:
                tx_df, summary_df, cluster_df = self.analyze_holder(holder)
                if not tx_df.empty:
                    all_transactions.append(tx_df)
                if not summary_df.empty:
                    all_summaries.append(summary_df)
                if not cluster_df.empty:
                    all_clusters.append(cluster_df)
            except Exception as e:
                logger.error(f"Error analyzing holder {holder}: {e}")
                continue

        combined_txs = self.safe_concat_dataframes(all_transactions, "transactions")
        combined_summaries = self.safe_concat_dataframes(all_summaries, "summaries")
        combined_clusters = self.safe_concat_dataframes(all_clusters, "clusters")
        return combined_txs, combined_summaries, combined_clusters

    # -------------------- 출력/저장 --------------------
    def generate_report(self, tx_df: pd.DataFrame, summary_df: pd.DataFrame, cluster_df: pd.DataFrame) -> str:
        report = []
        report.append("=" * 80)
        report.append("BELLE 토큰 허니팟 분석 보고서 (557개 홀더)")
        report.append("=" * 80)

        # 현재 시각 및 윈도우 표기
        report.append(f"분석 시간: {datetime.now(UTC).astimezone(KST).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        if self.window_start_utc or self.window_end_utc:
            s = self.window_start_utc.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S %Z') if self.window_start_utc else "-"
            e = self.window_end_utc.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S %Z') if self.window_end_utc else "-"
            report.append(f"적용 윈도우: {s} ~ {e}")
        report.append("")

        if summary_df.empty:
            report.append("분석할 데이터가 없습니다.")
            return "\n".join(report)

        total_holders = len(summary_df)
        total_txs = int(summary_df["tx_to_router"].sum())
        total_fails = int(summary_df["fails"].sum())
        overall_fail_rate = (total_fails / total_txs) if total_txs > 0 else 0.0
        total_gas_waste = float(summary_df["total_gas_waste_eth"].sum())

        report.append("📊 전체 통계:")
        report.append(f"  - 분석 홀더 수: {total_holders} / 557 (목표)")
        report.append(f"  - 총 라우터 거래(윈도우 내): {total_txs:,}")
        report.append(f"  - 실패 거래: {total_fails:,}")
        report.append(f"  - 전체 실패율: {overall_fail_rate:.1%}")
        report.append(f"  - 총 가스 낭비: {total_gas_waste:.4f} ETH")
        report.append("")

        indicators = self.detect_honeypot_indicators(summary_df)
        if indicators:
            report.append("🚨 허니팟 지표 탐지:")
            for holder, holder_indicators in indicators.items():
                report.append(f"  {holder[:10]}...:")
                for indicator in holder_indicators:
                    report.append(f"    ⚠️  {indicator}")
            report.append("")

        report.append("👥 홀더별 분석 결과:")
        for _, row in summary_df.iterrows():
            report.append(f"  {row['holder'][:10]}...:")
            report.append(
                f"    거래: {row['tx_to_router']:,} | 실패율: {row['fail_rate']:.1%} | "
                f"연속실패: {row['max_consecutive_fails']} | 매도실패율(전용): {row.get('sell_fail_rate',0):.1%} / {row.get('sell_txs',0)}건"
            )
            report.append(f"    가스낭비: {row['total_gas_waste_eth']:.4f} ETH | 클러스터: {row['clusters_2h_count']}")
        report.append("")

        # 결론(윈도우 한정)
        high_risk_holders = len([h for h, inds in indicators.items() if len(inds) >= 2])
        report.append("📋 분석 결론(윈도우 적용):")
        if high_risk_holders > 0:
            report.append(f"  🔴 HIGH RISK: {high_risk_holders}/{total_holders} 홀더가 허니팟 지표 다수 보유")
            report.append("      → 매도 차단 허니팟 스캠 토큰으로 판단됨")
        else:
            report.append("  🟡 일부 지표 존재하나 결정적 증거 부족")

        report.append("=" * 80)
        return "\n".join(report)

    def save_results(self, tx_df: pd.DataFrame, summary_df: pd.DataFrame, cluster_df: pd.DataFrame) -> Dict[str, str]:
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filenames = {
            "transactions": f"belle0_transactions_{timestamp}.csv",
            "summary": f"belle0_summary_{timestamp}.csv",
            "clusters": f"belle0_clusters_{timestamp}.csv",
            "report": f"belle0_report_{timestamp}.txt"
        }
        saved = {}
        try:
            if not tx_df.empty:
                tx_df.to_csv(filenames["transactions"], index=False)
                logger.info(f"Saved transactions to {filenames['transactions']}")
                saved["transactions"] = filenames["transactions"]

            if not summary_df.empty:
                summary_df.to_csv(filenames["summary"], index=False)
                logger.info(f"Saved summary to {filenames['summary']}")
                saved["summary"] = filenames["summary"]

            if not cluster_df.empty:
                cluster_df.to_csv(filenames["clusters"], index=False)
                logger.info(f"Saved clusters to {filenames['clusters']}")
                saved["clusters"] = filenames["clusters"]

            report = self.generate_report(tx_df, summary_df, cluster_df)
            with open(filenames["report"], 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"Saved report to {filenames['report']}")
            saved["report"] = filenames["report"]
            return saved

        except Exception as e:
            logger.error(f"Error saving results: {e}")
            return saved

# -------------------- 메인 --------------------
def main():
    config = Config()
    
    # 557개 홀더 분석을 위한 설정 확인
    logger.info(f"=== 설정 확인 ===")
    logger.info(f"목표 홀더 수: {config.HOLDER_LIMIT}개")
    logger.info(f"배치 크기: {config.BATCH_SIZE}")
    logger.info(f"요청 지연: {config.REQUEST_DELAY}초")
    logger.info(f"예상 소요 시간: {config.HOLDER_LIMIT * config.REQUEST_DELAY / 60:.1f}분 이상")
    
    analyzer = EtherscanAnalyzer(
        api_key=os.getenv("ETHERSCAN_API_KEY", "YOUR_API_KEY"),
        config=config
    )

    token_addr = os.getenv("TOKEN_ADDRESS", "").strip()
    if not token_addr:
        raise SystemExit("환경변수 TOKEN_ADDRESS 가 필요합니다. 예) set TOKEN_ADDRESS=0x...")

    # 상위 557개 홀더 자동 수집
    logger.info(f"Fetching top {config.HOLDER_LIMIT} holders for token: {token_addr}")
    holders = analyzer.get_holders(token_addr, limit=config.HOLDER_LIMIT)
    if not holders:
        raise SystemExit("홀더 목록을 가져오지 못했습니다. TOKEN_ADDRESS/API 키/쿼터를 점검하세요.")
    
    logger.info(f"Successfully fetched {len(holders)} holders")
    logger.info(f"First 10 holders: {holders[:10]}")
    logger.info(f"Last 10 holders: {holders[-10:]}")

    # 분석 실행 (557개 홀더 전부)
    logger.info("=== 557개 홀더 분석 시작 ===")
    tx_df, summary_df, cluster_df = analyzer.analyze_multiple_holders(holders)

    # 출력
    print("\n" + analyzer.generate_report(tx_df, summary_df, cluster_df))

    if not summary_df.empty:
        print("\n=== 상세 요약 (상위 50개) ===")
        show = summary_df.sort_values(["tx_to_router", "fails"], ascending=False).head(50)
        print(show.to_string(index=False))

    if not tx_df.empty:
        print("\n=== 최근 실패 거래 TOP 20 ===")
        recent_fails = tx_df[tx_df["status"] == "fail"].sort_values("time_utc", ascending=False).head(20)
        cols = ["holder", "time_utc", "method_guess", "gas_fee_eth"]
        print(recent_fails[cols].to_string(index=False))

    # 저장 (파일명에 557 추가)
    saved_files = analyzer.save_results(tx_df, summary_df, cluster_df)
    if saved_files:
        print(f"\n📁 저장된 파일들 (557개 홀더 분석):")
        for k, v in saved_files.items():
            print(f"  - {k}: {v}")
    
    print("\n=== 분석 완료 ===")
    print(f"총 분석 홀더 수: {len(holders)}개")
    print(f"총 트랜잭션 수: {len(tx_df):,}개" if not tx_df.empty else "총 트랜잭션 수: 0개")

if __name__ == "__main__":
    main()
