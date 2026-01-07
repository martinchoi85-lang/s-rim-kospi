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