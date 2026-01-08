# s-rim-kospi
A project to evaluate stock prices (whether they are expensive or cheap) for all stocks in the KOSPI market (and ultimately KOSDAQ) based on the S-RIM model.

Command

API server
>> uvicorn app.api.main:app --reload

Web App server
> streamlit run streamlit_app.py


전체 적재+계산(날짜는 해당 Quarter 아무 날이나 가능):
>> python -m app.etl.run_etl --as-of-date 2026-01-31 --stages 0,1,2,3 --r 0.10

계산만 재실행(매월 시장가만 새로 반영하고 계산(재무는 그대로)):
>> python -m app.etl.run_etl --snapshot-id 2026Q1 --stages 3
(as-of는 snapshot_id가 주어지면 사실상 참고값이지만, CLI 일관성을 위해 남겨둠)



Score 설계 (MVP)

추천 후보군: quality=OK + gap_pct > 0 + (옵션) roe_derived > 0

스코어 구성 요소:
gap_pct: 클수록 좋음
roe_derived: 클수록 좋음 (단, outlier 방지 필요)
pbr_derived: 낮을수록 좋음 (가능할 때만)


정규화(간단/안전):
gap_score = clip(gap_pct, 0, 200) / 200
roe_score = clip(roe_derived, -0.1, 0.3) -> 0~1로 스케일
pbr_score = 1 - clip(pbr_derived, 0, 5) / 5 (pbr 낮을수록 ↑)

최종 점수:
score = w_gap*gap_score + w_roe*roe_score + w_pbr*pbr_score - penalty(flags)

패널티:
WARN/EXCLUDE는 기본 후보에서 제외(또는 패널티 크게)