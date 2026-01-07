로드맵(추후 구현용) — DB/ETL/API/UI까지 연결한 형태로 반영
Phase 1. “추천 후보군”을 제대로 만들기 (가장 효과 큼)

후보군 = quality=OK AND gap_pct >= X AND (옵션) market_cap >= Y, sector 제외, 거래정지/관리종목 제외

UI: “추천 탭”에서 체크박스/슬라이더로 후보군 조건 조절

UI: gap_pct, market_cap, roe 등 다중 정렬(sort) 제공

Phase 2. 추가 투자지표(핵심 재무/밸류) 적재

추가 지표 예:

PBR = 시가총액 / 자본총계

PER = 시가총액 / 순이익 (적자면 제외/주의)

PSR, EV/EBITDA(가능하면)

부채비율, 이자보상배율, 영업현금흐름 등

저장 방식(부담 최소):

(추천) metrics_snapshot(snapshot_id, ticker, pbr, per, debt_ratio, ... , jsonb flags) 신규 테이블 1개

또는 fundamental_snapshot.data_quality에 계속 우겨 넣지 말고 “지표는 지표 테이블”로 분리 (조회/정렬/인덱스에 유리)

Phase 3. 추천 스코어링(Composite Score)

예:

Score = w1 * normalize(gap_pct) + w2 * normalize(roe) − w3 * normalize(debt_ratio) − 패널티(flags)

UI: 가중치 슬라이더(학습용) + “Top N 추천” 버튼

Phase 4. UI 고도화(리서치 툴 수준)

컬럼 선택/숨김

다중 정렬(Primary/Secondary sort)

조건 저장(“내 스크리너 프리셋”)

ticker 상세에서 “지표 히스토리(스냅샷별)” 비교