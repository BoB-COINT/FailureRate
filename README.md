# FailureRate

스캠(허니팟) 토큰에서 **매도 실패율(Sell Failure Rate, SFR)** 을 계산·리포팅해 “출구(매도) 차단” 징후를 정량적으로 확인하는 리포지토리입니다.  
DEX(주로 Uniswap V2/V3) 스왑 트랜잭션을 수집·필터링하여 **매도 시도 중 실패한 스왑의 비율**을 산출하고, CSV/LOG/텍스트 리포트를 생성합니다.

---

## 왜 ‘매도 실패율’인가?

허니팟/스캠 토큰은 보통 다음 중 하나로 **매도 경로를 차단**합니다.

- **Sell-Path Block / Conditional Revert**: 매도 시 특정 조건에서 `revert`
- **High Sell Tax / Fee Bomb**: 과도한 판매 세금으로 실질적 매도 불가
- **Blacklist / Whitelist-gated**: 특정 주소만 매도 허용, 나머지는 실패
- **Router/Pair 조작**: 가짜 페어, 임의 라우터 교체 등으로 스왑 실패 유도

이 결과로 온체인에서는 **비슷한 시각대에 다수의 매도 실패가 군집**하는 패턴이 나타납니다. 

---

## 내부 트랜잭션으로 실패를 확인할 수 있는 경우 / 없는 경우

- **가능한 경우**
  - 외부 트랜잭션은 성공이지만, 내부 호출 중 조건 분기에서 `revert` → 내부 호출/이벤트 단서가 남는 사례가 있음.
  - 일부 라우터/페어 구현은 실패 원인을 이벤트/로그로 노출.

- **불가능한 경우**
  - **외부 트랜잭션 자체가 `Status: Fail`** 로 롤백되면 내부 호출 기록이 남지 않을 수 있음.
  - 페어 컨트랙트의 **Swap 이벤트 미발생**(=체결 없음)으로만 간접 확인해야 할 때가 있음.

따라서 본 도구는 **외부 트랜잭션의 status**와 **페어의 Swap 이벤트 존재/부재**를 함께 보며 매도 실패를 집계합니다.

---

## 리포지토리 구조 
`````
├─ BELLE/
│ ├─ .py, .txt, .csv
├─ $88/
├─ main.py
└─ README.md
`````

---

## 설치 / 준비

```bash
# (선택) 가상환경
python -m venv .venv
# Windows
.venv\Scripts\activate

# 필요 패키지 설치
pip install requests pandas python-dotenv
```

## 사용법

```bash
# 현재 세션에만 적용
$env:ETHERSCAN_API_KEY = "etherscan_실제_API_키_입력"
$env:TOKEN_ADDRESS = "0x토큰주소"
$env:TOKEN_NAME        = "토큰이름"
$env:HOLDER_LIMIT      = "홀더수" 

# 환경변수 확인
echo $env:ETHERSCAN_API_KEY
echo $env:TOKEN_ADDRESS
echo $env:TOKEN_NAME
echo $env:HOLDER_LIMIT

# 실행
python main.py 
```

---

## 출력물

| 파일                   | 설명                                                            |
| ---------------------- | -------------------------------------------------------------- |
| `*_transactions_*.csv` | 수집된 스왑 트xn(시간, from/to, 메서드, status, amountIn/out 등) |
| `*_summary_*.csv`      | 요약 통계(총 매도/매수 시도, 실패/성공 수, 실패율, 기간 등)       |
| `*_clusters_*.csv`     | 시계열/주소별 군집 결과(동시 다발 실패 군집 탐지)                 |
| `*_analysis.log`       | 실행 로그(필터/예외, API 회수, 경고 등)                          |
| `*_report_*.txt`       | 텍스트 리포트(핵심 지표/판단 가이드)                             |

---

## 산출 지표 (초안)

Sell Failure Rate (SFR)

- 0.00 ~ 0.05 : 일반(슬리피지/가스/혼잡 등 자연 실패 수준)
- 0.05 ~ 0.20 : 주의(특정 시점 군집·특정 주소군 집중 시 의심)
- ~ > 0.20 : 경고(허니팟 가능성 큼—다른 지표와 함께 교차 확인 권장)

* 수치는 데이터셋·기간에 따라 달라질 수 있음.
* 정상 홀더의 거래량이 매도 실패한 홀더의 거래량보다 훨씬 많기 때문에, 한 홀더의 실패율이 높거나 비정상적으로 실패한 홀더가 많아도 전체 수치가 낮게 나올 수 있음.
