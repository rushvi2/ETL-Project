# patients: PatientID (PK), FirstName, LastName, DOB, Gender. (Chinook Employees sql table)
# doctors: DoctorID (PK), FirstName, LastName, Specialization. (data.cms api)
# appointments:  AppointmentID (PK), PatientID (FK), DoctorID (FK), AppointmentDate.
# medical records: RecordID (PK), PatientID (FK), VisitDate, DoctorID (FK), Diagnosis, Treatment. (diagnosis from inpatient charges, treatment from careplans)
# patient lifestyle: LifestyleID(PK), PatientID(FK), HeartRate, BloodPressure, BMI (ss.csv)
import pymysql.cursors
import warnings
import pandas as pd
import os
import json
import pprint
import requests
import requests.exceptions
from sqlalchemy import create_engine


host_ip = "127.0.0.1"
user_id = "root"
pwd = "umm9ef!!"

warnings.filterwarnings('ignore')

conn = pymysql.connect(host=host_ip, user=user_id, password=pwd, database='employees')
cursor = conn.cursor()

try:

    cursor.execute("CREATE DATABASE IF NOT EXISTS healthcare")
    conn.commit()
    cursor.execute("USE healthcare")
    # drop all tables everytime you run so no duplicate data
    cursor.execute("DROP TABLE IF EXISTS patient_lifestyle")
    cursor.execute("DROP TABLE IF EXISTS medical_records")
    cursor.execute("DROP TABLE IF EXISTS appointments")
    cursor.execute("DROP TABLE IF EXISTS patients")
    cursor.execute("DROP TABLE IF EXISTS doctors")
    conn.commit()
    print("All tables dropped")
    #add 5 tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS patients (
        PatientID INT AUTO_INCREMENT PRIMARY KEY,
        FirstName VARCHAR(14) NOT NULL,
        LastName VARCHAR(16) NOT NULL,
        DOB DATE NOT NULL,
        Gender ENUM('M', 'F') NOT NULL
    )
    """)
    conn.commit()
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS doctors (
                DoctorID INT AUTO_INCREMENT PRIMARY KEY,
                FirstName VARCHAR(50),
                LastName VARCHAR(50),
                Specialization VARCHAR(100)
            )
        """)
    conn.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS appointments (
            AppointmentID INT AUTO_INCREMENT PRIMARY KEY,
            PatientID INT,
            DoctorID INT,
            AppointmentDate DATE,
            FOREIGN KEY (PatientID) REFERENCES patients(PatientID),
            FOREIGN KEY (DoctorID) REFERENCES doctors(DoctorID)
        )
        """)
    conn.commit()
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS medical_records (
                RecordID INT AUTO_INCREMENT PRIMARY KEY,
                PatientID INT,
                VisitDate DATE,
                DoctorID INT,
                Diagnosis TEXT,
                Treatment TEXT,
                FOREIGN KEY (PatientID) REFERENCES patients(PatientID),
                FOREIGN KEY (DoctorID) REFERENCES doctors(DoctorID)
            )
        """)
    conn.commit()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS patient_lifestyle (
            LifestyleID INT AUTO_INCREMENT PRIMARY KEY,
            PatientID INT,
            HeartRate VARCHAR(50),
            BloodPressure VARCHAR(50),
            BMI VARCHAR(50),
            FOREIGN KEY (PatientID) REFERENCES patients(PatientID)
        )
    """)
    conn.commit()


except Exception as e:
    print(f"Error: {e}")

cursor.close()
conn.close()

conn_employees = pymysql.connect(host=host_ip, user=user_id, password=pwd, database="employees")
cursor_employees = conn_employees.cursor()
conn_healthcare = pymysql.connect(host=host_ip, user=user_id, password=pwd, database="healthcare")
cursor_healthcare = conn_healthcare.cursor()
#fill patient table from employees sql table
try:
    cursor_employees.execute("SELECT birth_date, first_name, last_name, gender FROM employees LIMIT 100")
    employees_data = cursor_employees.fetchall()
    insert_query = """
    INSERT INTO patients (DOB, FirstName, LastName, Gender) VALUES (%s, %s, %s, %s)
    """
    for row in employees_data:
        dob, first_name, last_name, gender = row
        cursor_healthcare.execute(insert_query, (dob, first_name, last_name, gender))

    conn_healthcare.commit()

except Exception as e:
    print(f"Error: {e}")


cursor_employees.close()
conn_employees.close()
cursor_healthcare.close()
conn_healthcare.close()

#
# conn = pymysql.connect(host=host_ip, user=user_id, password=pwd, db="healthcare")
# sql_query = "SELECT * FROM patients"
# df_patients = pd.read_sql_query(sql_query, conn)
# conn.close()
# print(df_patients.head(50))

#connect to api
def get_api_response(url, response_type):
    try:
        response = requests.get(url)
        response.raise_for_status()

    except requests.exceptions.HTTPError as errh:
        return "An Http Error occurred: " + repr(errh)
    except requests.exceptions.ConnectionError as errc:
        return "An Error Connecting to the API occurred: " + repr(errc)
    except requests.exceptions.Timeout as errt:
        return "A Timeout Error occurred: " + repr(errt)
    except requests.exceptions.RequestException as err:
        return "An Unknown Error occurred: " + repr(err)

    if response_type == 'json':
        return response.json()
    elif response_type == 'dataframe':
        result = pd.json_normalize(response.json())
    else:
        result = "An unhandled error has occurred!"

    return result

url = "https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0?offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false"
response_type = ['json', 'dataframe']
data = get_api_response(url, response_type[0])

conn_healthcare = pymysql.connect(host=host_ip, user=user_id, password=pwd, database="healthcare")
cursor_healthcare = conn_healthcare.cursor()
try:
    #fill doctors table from api
    for doctor in data['results']:

        insert_query = """
        INSERT INTO doctors (FirstName, LastName, Specialization)
        VALUES (%s, %s, %s)
        """
        first_name = doctor['provider_first_name']
        last_name = doctor['provider_last_name']
        specialization = doctor['pri_spec']
        cursor_healthcare.execute(insert_query, (first_name, last_name, specialization))

    conn_healthcare.commit()

except Exception as e:
    print(f"Error: {e}")

cursor_healthcare.close()
conn_healthcare.close()

# conn = pymysql.connect(host=host_ip, user=user_id, password=pwd, db="healthcare")
# sql_query = "SELECT * FROM doctors"
# df_doctors = pd.read_sql_query(sql_query, conn)
# conn.close()
# print(df_doctors.head(50))

conn_healthcare = pymysql.connect(host=host_ip, user=user_id, password=pwd, database="healthcare", cursorclass=pymysql.cursors.DictCursor)
cursor_healthcare = conn_healthcare.cursor()
#convert csv file to sql table
careplans_df = pd.read_csv('data/careplans.csv')
engine = create_engine('mysql+pymysql://root:umm9ef!!@127.0.0.1/healthcare')
careplans_df.to_sql('careplans', con=engine, index=False, if_exists='replace')

#fill appointments table
try:
    cursor_healthcare.execute("SELECT PatientID FROM patients")
    patient_ids = [row['PatientID'] for row in cursor_healthcare.fetchall()]
    cursor_healthcare.execute("SELECT DoctorID FROM doctors")
    doctor_ids = [row['DoctorID'] for row in cursor_healthcare.fetchall()]
    if not patient_ids or not doctor_ids:
        raise ValueError("Patient or Doctor IDs not found.")

    cursor_healthcare.execute("SELECT START FROM careplans")
    appointment_dates = [row['START'] for row in cursor_healthcare.fetchall()]
    index = 0
    for appointment_date in appointment_dates:
        #rotate through possible ids
        patient_id = patient_ids[index % len(patient_ids)]
        doctor_id = doctor_ids[index % len(doctor_ids)]
        index += 1
        insert_query = "INSERT INTO appointments (PatientID, DoctorID, AppointmentDate) VALUES (%s, %s, %s)"
        cursor_healthcare.execute(insert_query, (patient_id, doctor_id, appointment_date))

    conn_healthcare.commit()
except Exception as e:
    print(f"Error inserting appointments: {e}")

cursor_healthcare.close()
conn_healthcare.close()


# conn = pymysql.connect(host=host_ip, user=user_id, password=pwd, db="healthcare")
# sql_query = "SELECT * FROM appointments"
# df_apps = pd.read_sql_query(sql_query, conn)
# conn.close()
# print(df_apps.head(50))


inpatient_df = pd.read_csv('data/inpatientCharges.csv')
inpatient_df.columns = inpatient_df.columns.str.strip().str.replace(' ', '_')
inpatient_df.to_sql('inpatientCharges', con=engine, index=False, if_exists='replace')


conn_healthcare = pymysql.connect(host=host_ip, user=user_id, password=pwd, database="healthcare", cursorclass=pymysql.cursors.DictCursor)
cursor_healthcare = conn_healthcare.cursor()

try:
    cursor_healthcare.execute("SELECT `DRG_Definition` FROM inpatientCharges")
    diagnoses = [row['DRG_Definition'] for row in cursor_healthcare.fetchall()]
    cursor_healthcare.execute("SELECT `START`, `DESCRIPTION` FROM careplans")
    care_plans = [{'visit_date': row['START'], 'treatment': row['DESCRIPTION']} for row in cursor_healthcare.fetchall()]
    min_length = min(len(diagnoses), len(care_plans), len(patient_ids), len(doctor_ids))

    for index in range(min_length):
        patient_id = patient_ids[index % len(patient_ids)]
        doctor_id = doctor_ids[index % len(doctor_ids)]
        diagnosis = diagnoses[index]
        treatment = care_plans[index]['treatment']
        visit_date = care_plans[index]['visit_date']
        insert_query = """
            INSERT INTO medical_records (PatientID, DoctorID, VisitDate, Diagnosis, Treatment)
            VALUES (%s, %s, %s, %s, %s)
            """
        cursor_healthcare.execute(insert_query, (patient_id, doctor_id, visit_date, diagnosis, treatment))
    conn_healthcare.commit()
except Exception as e:
    print(f"Error inserting medical records: {e}")

cursor_healthcare.close()
conn_healthcare.close()

# conn = pymysql.connect(host=host_ip, user=user_id, password=pwd, db="healthcare")
# sql_query = "SELECT * FROM medical_records"
# df_apps = pd.read_sql_query(sql_query, conn)
# conn.close()
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# print(df_apps.head(50))

ss_df = pd.read_csv('data/ss.csv')
ss_df.columns = ss_df.columns.str.strip().str.replace(' ', '_')
ss_df.to_sql('ss', con=engine, index=False, if_exists='replace')
conn_healthcare = pymysql.connect(host=host_ip, user=user_id, password=pwd, database="healthcare", cursorclass=pymysql.cursors.DictCursor)
cursor_healthcare = conn_healthcare.cursor()

try:
    cursor_healthcare.execute("SELECT `Heart_Rate`, `Blood_Pressure`, `BMI_Category` FROM ss")
    ss = cursor_healthcare.fetchall()
    for index, row in enumerate(ss):
        patient_id = patient_ids[index % len(patient_ids)]
        heart_rate = row['Heart_Rate']
        blood_pressure = row['Blood_Pressure']
        bmi = row['BMI_Category']
        insert_query = """
            INSERT INTO patient_lifestyle (PatientID, HeartRate, BloodPressure, BMI)
            VALUES (%s, %s, %s, %s)
            """
        cursor_healthcare.execute(insert_query, (patient_id, heart_rate, blood_pressure, bmi))

    conn_healthcare.commit()
except Exception as e:
    print(f"Error inserting patient lifestyle data: {e}")



# conn = pymysql.connect(host=host_ip, user=user_id, password=pwd, db="healthcare")
# sql_query = "SELECT * FROM patient_lifestyle"
# df_apps = pd.read_sql_query(sql_query, conn)
# conn.close()
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# print(df_apps.head(50))


conn_healthcare = pymysql.connect(host=host_ip, user=user_id, password=pwd, database="healthcare", cursorclass=pymysql.cursors.DictCursor)
cursor_healthcare = conn_healthcare.cursor()

try:
    query = """
    SELECT 
        p.FirstName AS PatientFirstName, 
        p.LastName AS PatientLastName, 
        d.FirstName AS DoctorFirstName, 
        d.LastName AS DoctorLastName, 
        a.AppointmentDate
    FROM appointments a
    JOIN patients p ON a.PatientID = p.PatientID
    JOIN doctors d ON a.DoctorID = d.DoctorID
    LIMIT 50;
    """
    cursor_healthcare.execute(query)
    for row in cursor_healthcare.fetchall():
        print(row)
except Exception as e:
    print(e)
try:
    query = """
    SELECT 
        p.PatientID, 
        p.FirstName, 
        p.LastName, 
        COUNT(a.AppointmentID) AS NumberOfAppointments
    FROM appointments a
    JOIN patients p ON a.PatientID = p.PatientID
    GROUP BY p.PatientID;
    """
    cursor_healthcare.execute(query)
    for row in cursor_healthcare.fetchall():
        print(row)
except Exception as e:
    print(e)
