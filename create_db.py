import pandas as pd
import sqlite3

def create_db():
    """
    Creates a local database, student.db, from csv files.
    Ensure that csv files are in directory from which script is executed.

    Args:
        none

    Returns:
        none
    """

    enr = pd.read_csv('fall_enrollment.csv', dtype={'id': str, 'term_code': str})
    grades = pd.read_csv('grades.csv')
    program = pd.read_csv('program_data.csv')
    ratings = pd.read_csv('admission_rating.csv', dtype={'id': str})

    conn = sqlite3.connect('student.db')
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS enr (
            id TEXT,
            term_code TEXT,
            census TEXT,
            race_desc TEXT,
            legal_sex_desc TEXT,
            ethn_desc TEXT,
            visa_desc TEXT,
            college TEXT,
            degr TEXT,
            majr TEXT,
            birth_date DATE)""")

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS grades (
                id INTEGER,
                term_code INTEGER,
                final_course_grade TEXT)""")

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS program (
                    COLLEGE TEXT,
                    DEGREE TEXT,
                    MAJOR TEXT,
                    PROGRAM TEXT)""")

    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ratings (
                        id TEXT,
                        admit_rating INTEGER,
                        sequence_no INTEGER)""")

    for df, table in zip(
            [enr, grades, program, ratings],
            ['enr', 'grades', 'program', 'ratings']):
        try:
            df.to_sql(table, conn, if_exists='replace', index=False)

        except Exception as e:
            print(e)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    create_db()
