import sqlite3
import pandas as pd

DB_PATH = "student.db"

SINGLE_QUERY = """
WITH
-- Deduplicate WK3 snapshot per student
wk3 AS (
  SELECT *
  FROM (
    SELECT
      id,
      term_code,
      census,
      race_desc,
      legal_sex_desc,
      ethn_desc,
      visa_desc,
      college,
      degr,
      majr,
      birth_date,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY term_code DESC) AS rn
    FROM enr
    WHERE census = 'WK3'
  )
  WHERE rn = 1
),
-- Deduplicate EOT snapshot per student
eot AS (
  SELECT *
  FROM (
    SELECT
      id,
      term_code,
      census,
      race_desc,
      legal_sex_desc,
      ethn_desc,
      visa_desc,
      college,
      degr,
      majr,
      birth_date,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY term_code DESC) AS rn
    FROM enr
    WHERE census = 'EOT'
  )
  WHERE rn = 1
),
-- Map college/degree/major -> program for WK3 and EOT
wk3_prog AS (
  SELECT
    w.*,
    p.PROGRAM AS wk3_program
  FROM wk3 w
  LEFT JOIN program p
    ON p.COLLEGE = w.college
   AND p.DEGREE  = w.degr
   AND p.MAJOR   = w.majr
),
eot_prog AS (
  SELECT
    e.*,
    p.PROGRAM AS eot_program
  FROM eot e
  LEFT JOIN program p
    ON p.COLLEGE = e.college
   AND p.DEGREE  = e.degr
   AND p.MAJOR   = e.majr
),
-- Convert letter grades -> numeric GPA, then aggregate per student
grade_map AS (
  SELECT
    g.id,
    CASE TRIM(g.final_course_grade)
      WHEN 'A'  THEN 4.0
      WHEN 'A-' THEN 3.7
      WHEN 'B+' THEN 3.3
      WHEN 'B'  THEN 3.0
      WHEN 'B-' THEN 2.7
      WHEN 'C+' THEN 2.3
      WHEN 'C'  THEN 2.0
      WHEN 'C-' THEN 1.7
      WHEN 'D+' THEN 1.3
      WHEN 'D'  THEN 1.0
      WHEN 'F'  THEN 0.0
      ELSE NULL
    END AS gpa_num
  FROM grades g
),
gpa_by_student AS (
  SELECT
    id,
    AVG(gpa_num)           AS avg_gpa,
    MIN(gpa_num)           AS min_gpa,
    MAX(gpa_num)           AS max_gpa,
    COUNT(gpa_num)         AS num_courses
  FROM grade_map
  GROUP BY id
),
-- Most recent admission rating per student by highest sequence_no
latest_rating AS (
  SELECT id,
         CAST(admit_rating AS TEXT) AS admit_rating_text
  FROM (
    SELECT
      id,
      admit_rating,
      sequence_no,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY sequence_no DESC) AS rn
    FROM ratings
  )
  WHERE rn = 1
),
-- Base set of students (anyone appearing in enrollment table)
all_ids AS (
  SELECT DISTINCT id FROM enr
)
SELECT
  a.id,

  -- Enrollment presence flags (census-level)
  CASE WHEN w.id IS NOT NULL THEN 1 ELSE 0 END AS enrolled_wk3,
  CASE WHEN e.id IS NOT NULL THEN 1 ELSE 0 END AS enrolled_eot,
  CASE WHEN w.id IS NOT NULL AND e.id IS NOT NULL THEN 1 ELSE 0 END AS persisted_wk3_to_eot,

  -- Demographics
  COALESCE(e.legal_sex_desc, w.legal_sex_desc) AS legal_sex_desc,
  -- Race/Ethnicity precedence: visa -> hispanic -> race -> Unknown
  CASE
    WHEN COALESCE(UPPER(e.visa_desc), UPPER(w.visa_desc)) IS NOT NULL
         AND TRIM(COALESCE(UPPER(e.visa_desc), UPPER(w.visa_desc))) <> ''
         AND TRIM(COALESCE(UPPER(e.visa_desc), UPPER(w.visa_desc))) NOT IN ('PR','RF','AS')
      THEN 'International'
    WHEN LOWER(COALESCE(e.ethn_desc, w.ethn_desc)) LIKE '%hispanic%'
         AND LOWER(COALESCE(e.ethn_desc, w.ethn_desc)) NOT LIKE 'not hispanic%'
      THEN 'Hispanic or Latino'
    ELSE COALESCE(e.race_desc, w.race_desc, 'Unknown')
  END AS race_ethnicity,

  -- Birth date (as provided)
  COALESCE(e.birth_date, w.birth_date) AS birth_date,

  -- Curricular (snapshot-specific)
  w.college AS wk3_college,
  w.degr    AS wk3_degree,
  w.majr    AS wk3_major,
  w.wk3_program AS wk3_program,

  e.college AS eot_college,
  e.degr    AS eot_degree,
  e.majr    AS eot_major,
  e.eot_program AS eot_program,

  -- Final chosen curricular/program (prefer EOT)
  COALESCE(e.eot_program, w.wk3_program) AS program_final,
  COALESCE(e.degr, w.degr)               AS degree_final,
  COALESCE(e.college, w.college)         AS college_final,
  COALESCE(e.majr, w.majr)               AS major_final,

  -- GPA aggregates
  ROUND(g.avg_gpa, 3)  AS avg_gpa,
  g.min_gpa,
  g.max_gpa,
  g.num_courses,

  -- Admission rating (most recent); impute 'unk' if missing
  COALESCE(r.admit_rating_text, 'unk') AS latest_admit_rating

FROM all_ids a
LEFT JOIN wk3_prog w ON w.id = a.id
LEFT JOIN eot_prog e ON e.id = a.id
LEFT JOIN gpa_by_student g ON g.id = a.id
LEFT JOIN latest_rating r   ON r.id = a.id
ORDER BY a.id;
"""

def main():
    # Connect and run the single-query
    con = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(SINGLE_QUERY, con)
    finally:
        con.close()

    # Save and preview
    df.to_csv("single_query_results.csv", index=False)
    print("Rows:", len(df), "Cols:", len(df.columns))
    print(df.head(5).to_string(index=False))
    return df

if __name__ == "__main__":
    main()