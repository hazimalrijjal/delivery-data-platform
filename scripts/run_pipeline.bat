@echo off
REM Full pipeline on DuckDB. Run from the project folder with the venv active.
python data_gen\generate.py --out data\raw\batch_001 --seed 42 || exit /b 1
python ingest\load_raw.py --batch-dir data\raw\batch_001 || exit /b 1
cd dbt_project
dbt build --full-refresh --profiles-dir . || exit /b 1
cd ..
pytest -q
