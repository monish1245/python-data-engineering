"""
HR Data Processing — Python Scripts
Author: Monish D P | Gaininsight Solutions
Description: Python utilities for HR data cleaning, analysis, and ETL automation
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import warnings
warnings.filterwarnings('ignore')


# ─────────────────────────────────────────────────────────────
# 1. CSV DATA CLEANER — handles missing values
# ─────────────────────────────────────────────────────────────

def clean_hr_data(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Cleans HR employee CSV data:
    - Fills missing phone numbers with 'Not Provided'
    - Fills missing emails with 'no.email@company.com'
    - Strips whitespace from string fields
    - Converts date columns to datetime
    
    Args:
        input_file: Path to raw CSV file
        output_file: Path to cleaned output CSV
    Returns:
        Cleaned DataFrame
    """
    df = pd.read_csv(input_file)
    
    print(f"✅ Loaded {len(df)} records from {input_file}")
    print(f"📋 Missing values before cleaning:\n{df.isnull().sum()}\n")
    
    # Clean string fields
    string_cols = ['First_Name', 'Last_Name', 'Department', 'Location', 
                   'Employment_Status', 'Mob_No', 'Email_Address']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    # Handle missing contact info (mirrors Talend tMap logic)
    df['Mob_No'] = df['Mob_No'].replace({'nan': None, '': None})
    df['Mob_No'] = df['Mob_No'].fillna('Not Provided')
    
    df['Email_Address'] = df['Email_Address'].replace({'nan': None, '': None})
    df['Email_Address'] = df['Email_Address'].fillna('no.email@company.com')
    
    # Convert date columns
    date_cols = ['Join_Date', 'Exit_Date']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y')
    
    print(f"📋 Missing values after cleaning:\n{df.isnull().sum()}\n")
    
    df.to_csv(output_file, index=False)
    print(f"✅ Cleaned data saved to {output_file}")
    
    return df


# ─────────────────────────────────────────────────────────────
# 2. EMPLOYEE HEADCOUNT AGGREGATION
# ─────────────────────────────────────────────────────────────

def headcount_by_department(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates employee count and salary by department.
    Mirrors the Talend tAggregateRow job (Job 05).
    
    Args:
        df: Employee DataFrame
    Returns:
        Department-level summary DataFrame
    """
    summary = df.groupby('Department').agg(
        Employee_Count=('Employee_ID', 'count'),
        Total_Salary=('Salary', 'sum'),
        Avg_Salary=('Salary', 'mean')
    ).reset_index()
    
    summary['Avg_Salary'] = summary['Avg_Salary'].round(2)
    summary = summary.sort_values('Employee_Count', ascending=False)
    
    print("\n📊 Employee Headcount by Department:")
    print(summary.to_string(index=False))
    
    return summary


# ─────────────────────────────────────────────────────────────
# 3. ATTRITION ANALYSIS — Average Tenure Calculation
# ─────────────────────────────────────────────────────────────

def attrition_analysis(df: pd.DataFrame) -> dict:
    """
    Calculates average tenure for employees who have exited.
    Mirrors the Talend TalendDate.diffDate() job (Job 07).
    
    Args:
        df: Employee DataFrame with Join_Date and Exit_Date
    Returns:
        Dictionary with attrition metrics
    """
    exited = df[df['Employment_Status'].str.lower() == 'exited'].copy()
    
    # Calculate tenure in days (mirrors TalendDate.diffDate())
    exited['Tenure_Days'] = (exited['Exit_Date'] - exited['Join_Date']).dt.days
    exited['Tenure_Months'] = (exited['Tenure_Days'] / 30).round(1)
    
    metrics = {
        'Total_Exited': len(exited),
        'Avg_Tenure_Days': round(exited['Tenure_Days'].mean(), 2),
        'Avg_Tenure_Months': round(exited['Tenure_Months'].mean(), 2),
        'Min_Tenure_Days': exited['Tenure_Days'].min(),
        'Max_Tenure_Days': exited['Tenure_Days'].max(),
        'Attrition_By_Dept': exited.groupby('Department')['Employee_ID'].count().to_dict()
    }
    
    print("\n📊 Attrition Analysis Results:")
    for k, v in metrics.items():
        print(f"  {k}: {v}")
    
    return metrics


# ─────────────────────────────────────────────────────────────
# 4. ATTENDANCE RATE CALCULATOR
# ─────────────────────────────────────────────────────────────

def attendance_analysis(attendance_df: pd.DataFrame, threshold: float = 75.0) -> pd.DataFrame:
    """
    Calculates attendance rate per employee and flags low attendance.
    Mirrors the Talend attendance analysis job (Job 11).
    
    Args:
        attendance_df: DataFrame with Employee_ID, Employee_Name, Status, Department
        threshold: Attendance rate % below which to flag (default: 75%)
    Returns:
        DataFrame with attendance rates and flags
    """
    summary = attendance_df.groupby(['Employee_ID', 'Employee_Name', 'Department']).agg(
        Present_Days=('Status', lambda x: (x == 'Present').sum()),
        Total_Days=('Status', 'count')
    ).reset_index()
    
    summary['Attendance_Rate'] = (
        (summary['Present_Days'] / summary['Total_Days']) * 100
    ).round(1)
    
    summary['Attendance_Flag'] = summary['Attendance_Rate'].apply(
        lambda r: '🚨 LOW ATTENDANCE' if r < threshold else '✅ OK'
    )
    
    summary = summary.sort_values('Attendance_Rate')
    
    low_attendance = summary[summary['Attendance_Rate'] < threshold]
    
    print(f"\n📊 Attendance Analysis (Threshold: {threshold}%)")
    print(f"  Total Employees: {len(summary)}")
    print(f"  Low Attendance (<{threshold}%): {len(low_attendance)}")
    print("\n" + summary.to_string(index=False))
    
    return summary


# ─────────────────────────────────────────────────────────────
# 5. DEDUPLICATION — Remove Duplicate Employee_IDs
# ─────────────────────────────────────────────────────────────

def deduplicate_employees(df: pd.DataFrame, key_col: str = 'Employee_ID') -> tuple:
    """
    Removes duplicate records based on Employee_ID.
    Mirrors the Talend tUniqRow job (Job 10).
    
    Args:
        df: Employee DataFrame
        key_col: Column to use as dedup key (default: 'Employee_ID')
    Returns:
        Tuple of (clean_df, duplicate_df)
    """
    duplicates = df[df.duplicated(subset=[key_col], keep=False)]
    clean_df = df.drop_duplicates(subset=[key_col], keep='first')
    dupe_df = df[df.duplicated(subset=[key_col], keep='first')]
    
    print(f"\n🔍 Deduplication Results:")
    print(f"  Original records: {len(df)}")
    print(f"  Clean records:    {len(clean_df)}")
    print(f"  Duplicates found: {len(dupe_df)}")
    
    if len(dupe_df) > 0:
        print(f"\n  Duplicate Employee_IDs:")
        print(dupe_df[[key_col, 'First_Name', 'Last_Name', 'Department']].to_string(index=False))
    
    return clean_df, dupe_df


# ─────────────────────────────────────────────────────────────
# 6. DATA PROFILING REPORT
# ─────────────────────────────────────────────────────────────

def data_profile_report(df: pd.DataFrame) -> None:
    """
    Generates a quick data quality report for any HR DataFrame.
    
    Args:
        df: Employee DataFrame
    """
    print("=" * 60)
    print("📋 DATA PROFILING REPORT")
    print("=" * 60)
    print(f"Total Records:    {len(df)}")
    print(f"Total Columns:    {len(df.columns)}")
    print(f"Columns:          {list(df.columns)}")
    print()
    
    print("── NULL / MISSING VALUES ──")
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        pct = round(count / len(df) * 100, 1)
        status = "🚨" if pct > 10 else "✅"
        print(f"  {status} {col}: {count} missing ({pct}%)")
    
    print()
    print("── NUMERIC SUMMARY ──")
    print(df.describe().round(2).to_string())
    
    print()
    print("── CATEGORICAL DISTRIBUTIONS ──")
    cat_cols = df.select_dtypes(include='object').columns
    for col in cat_cols:
        if df[col].nunique() <= 10:
            print(f"\n  {col}:")
            print(df[col].value_counts().to_string())
    
    print("=" * 60)


# ─────────────────────────────────────────────────────────────
# MAIN — Demo run
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🚀 HR Data Processing Pipeline — Monish D P")
    print("   Gaininsight Solutions | Kioxia Project\n")
    
    # Sample dataset (mirrors real internship data structure)
    sample_data = {
        'Employee_ID': [101, 102, 103, 104, 105, 103],  # 103 is a duplicate
        'First_Name': ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Bob'],
        'Last_Name': ['Doe', 'Smith', 'Brown', 'Johnson', 'Wilson', 'Brown'],
        'Department': ['IT', 'HR', 'Finance', 'IT', 'HR', 'Finance'],
        'Join_Date': ['01/15/2020', '03/22/2019', '06/10/2021', '09/14/2020', '11/01/2022', '06/10/2021'],
        'Exit_Date': [None, '02/28/2023', None, None, None, None],
        'Salary': [80000, 70000, 65000, 90000, 55000, 65000],
        'Employment_Status': ['Active', 'Exited', 'Active', 'Active', 'Active', 'Active'],
        'Location': ['New York', 'Chicago', 'Los Angeles', 'San Francisco', 'Miami', 'Los Angeles'],
        'Mob_No': ['1234567890', None, '9876543210', '5551234567', '', '9876543210'],
        'Email_Address': ['john.doe@email.com', None, 'bob.brown@email.com', 'alice.j@email.com', 'charlie@email.com', 'bob.brown@email.com']
    }
    
    df = pd.DataFrame(sample_data)
    df['Join_Date'] = pd.to_datetime(df['Join_Date'], format='%m/%d/%Y')
    df['Exit_Date'] = pd.to_datetime(df['Exit_Date'], format='%m/%d/%Y', errors='coerce')
    
    # Run pipeline steps
    data_profile_report(df)
    clean_df, dupes = deduplicate_employees(df)
    headcount_by_department(clean_df)
    attrition_analysis(clean_df)
    
    print("\n✅ Pipeline complete!")
