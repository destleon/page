import datetime
import pyodbc
import pandas as pd
from openpyxl import load_workbook
import textdistance

FILE_PATH = 'ACC7 GRADING SHEET.xlsx'
WEEK = 12


COHORT = 'ACC7'
COHORT_ID = 25

EMAIL = 'Student Email'
QUIZ_SCORE = 'KC Score'
LAB_SUBMISSION = ''
CHECK_IN = 'Attendance Score'
PEER = ''
WATCH_LIST = 'Watchlist'
WATCH_LIST_EXPLAN = 'Watchlist Explanation'
DROPPED = "Dropped"
DROPPED_TYPE_ID = "Drop Reason"
DROPPED_EXPLAN = "Drop Explanation"


def connect_to_sql():
    server = "azsql-pa-01.database.windows.net"
    database = "Azubi_Dashboards"
    username = "sw_admin"
    password = "BIuErba(BoQ3"
    port = "1433"
    driver = "SQL Server"

    cnxn = pyodbc.connect(
        "DRIVER={SQL Server};SERVER="
        + server
        + ";DATABASE="
        + database
        + ";UID="
        + username
        + ";PWD="
        + password
    )
    return cnxn.cursor(), cnxn


def clean_string(some_string):
    some_string = some_string[:1999]
    return some_string.replace('\'', '\'\'')


def get_value(row_identifier, row):
    val = row[row_identifier]
    return clean_string(val) if isinstance(val, str) else None


def correct_text(text, df, col_name):
    options = list(df[col_name])
    for option in options:
        if textdistance.levenshtein.distance(option, text) < 2:
            text = option
    return text


def lookup_student(s, cohort_id, df):
    s = s.lower()
    s = correct_text(s, df, 'azubi_email')
    try:
        the_id = df.loc[(df.azubi_email == s) & (
            df.cohort_id == cohort_id)]['student_id'].values[0]
    except IndexError:
        return None
    return int(the_id)


def lookup_grade(s, df):
    try:
        the_id = df.loc[(df.student_id == s) & (
            df.week == WEEK)]['weekly_grade_id'].values[0]
    except IndexError:
        return None
    return int(the_id)


def get_int(someval):
    return None if pd.isna(someval) else int(someval)


def get_date(cnxn):
    cohort_df = pd.read_sql("SELECT * FROM Cohort", cnxn)
    the_id = COHORT_ID
    date = cohort_df.loc[cohort_df['cohort_id']
                         == the_id]['starting_date'].values[0]
    date = datetime.datetime.strptime(date, '%Y-%m-%d')
    date = date + datetime.timedelta(weeks=WEEK)
    return date


def insert_grade(attendance_score, contribution_score, quiz_score, watch_list, watch_list_explan, date, self_rating_contribution, self_rating_happiness, current_challenges, dropped, dropped_id, student_id, drop_explan, atu_form, cursor):
    sql_statement = """
                    INSERT INTO Weekly_Grades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    cursor.execute(sql_statement, (attendance_score, contribution_score, quiz_score, watch_list, watch_list_explan,
                                   date, WEEK, self_rating_contribution, self_rating_happiness, current_challenges, dropped, dropped_id, student_id, drop_explan, atu_form))
    cursor.commit()


def update_grade(grade_id, attendance_score, contribution_score, quiz_score, watch_list, watch_list_explan, date, self_rating_contribution, self_rating_happiness, current_challenges, dropped, dropped_id, student_id, drop_explan, atu_form, cursor):
    sql_statement = f"""
                    UPDATE Weekly_Grades
                    SET attendance_score = ?,
                    contribution_score = ?,
                    quiz_score = ?,
                    watch_list = ?,
                    watch_list_explan = ?,
                    date = ?,
                    week = ?,
                    dropped = ?,
                    dropped_type_id = ?,
                    student_id = ?,
                    dropped_explan = ?,
                    atu_form = ?
                    WHERE weekly_grade_id = '{grade_id}'"""
    cursor.execute(sql_statement, (attendance_score, contribution_score, quiz_score, watch_list, watch_list_explan, date, WEEK,
                                   dropped, dropped_id, student_id, drop_explan, atu_form))
    cursor.commit()

#########################


def upload_grades(grades, cursor, student_df, grade_df, date):
    for _, row in grades.iterrows():
        student_id = lookup_student(row[EMAIL], COHORT_ID, student_df)

        if student_id:
            student_email = row[EMAIL]
            grade_id = lookup_grade(student_id, grade_df)
            attendance_score = row[CHECK_IN]
            contribution_score = None
            quiz_score = row[QUIZ_SCORE]
            watch_list = bool(row[WATCH_LIST])
            watch_list_explan = get_value(WATCH_LIST_EXPLAN, row)
            dropped = bool(row[DROPPED])
            dropped_id = get_int(row[DROPPED_TYPE_ID])
            drop_explan = get_value(DROPPED_EXPLAN, row)
            self_rating_happiness = None
            self_rating_contribution = None
            current_challenges = None
            atu_form = None

            if grade_id is None:
                print('[INFO] INSERTING: ', student_email)
                insert_grade(attendance_score, contribution_score, quiz_score, watch_list, watch_list_explan, date,
                         self_rating_contribution, self_rating_happiness, current_challenges, dropped, dropped_id, student_id, drop_explan, atu_form, cursor)

            else:
                print('[INFO] UPDATING: ', student_email)
                update_grade(grade_id, attendance_score, contribution_score, quiz_score, watch_list, watch_list_explan, date,
                         self_rating_contribution, self_rating_happiness, current_challenges, dropped, dropped_id, student_id, drop_explan, atu_form, cursor)

            if dropped:
                print('[INFO] Student has dropped: ', student_email)
                sql_statement = "UPDATE Student SET dropped = ?, dropped_type_id = ? WHERE student_id = ? AND cohort_id = ?"
                cursor.execute(
                    sql_statement, (dropped, dropped_id, student_id, COHORT_ID))
                cursor.commit()

def main():
    cursor, cnxn = connect_to_sql()
    student_df = pd.read_sql(
        "SELECT student_id, azubi_email, cohort_id FROM Student", cnxn)
    grade_df = pd.read_sql(
        "SELECT weekly_grade_id, week, student_id FROM Weekly_Grades", cnxn)
    date = get_date(cnxn)

    grades_to_input = pd.read_excel(FILE_PATH)
    print('[INFO] Updating Grades Now!')
    upload_grades(grades_to_input, cursor, student_df, grade_df, date)


if __name__ == "__main__":
    main()
