# DU Technical Assessment — Single SQL Query

## Run
1) Install deps:
   pip install -r requirements.txt

2) Place CSVs in this folder:
   - fall_enrollment.csv
   - grades.csv
   - program_data.csv
   - admission_rating.csv

3) Build DB, then run the single query:
   python create_db.py
   python run_single_query.py

Outputs:
- student.db
- single_query_results.csv (pandas DataFrame export)

Notes:
- The SQL is a single statement (WITH CTEs) that produces one row per student, including:
  - WK3/EOT enrollment flags and persistence
  - computed race/ethnicity (visa → hispanic → race → 'Unknown')
  - WK3/EOT curricular + mapped program, plus 'final' (prefer EOT)
  - per-student avg/min/max GPA and course count
  - latest admission rating by sequence_no (or 'unk' if missing)
