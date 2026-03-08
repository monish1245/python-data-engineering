# 🐍 Python HR Data Processing Scripts

> Python utilities for HR data cleaning, ETL automation, and analytics  
> **Author:** Monish D P | Gaininsight Solutions

## Scripts

| File | Description |
|------|-------------|
| `hr_data_processing.py` | Main pipeline — cleaning, dedup, aggregation, attrition, attendance |

## Functions

| Function | What it does | Talend Equivalent |
|----------|-------------|-------------------|
| `clean_hr_data()` | Null handling for phone/email | Job 02 — Data Cleaning |
| `headcount_by_department()` | Employee count + salary by dept | Job 05 — Aggregation |
| `attrition_analysis()` | Avg tenure for exited staff | Job 07 — Attrition |
| `attendance_analysis()` | Flag <75% attendance | Job 11 — Attendance |
| `deduplicate_employees()` | Remove duplicate Employee_IDs | Job 10 — Dedup |
| `data_profile_report()` | Data quality summary report | — |

## How to Run

```bash
pip install pandas numpy
python hr_data_processing.py
```

## Sample Output

```
📋 DATA PROFILING REPORT
Total Records:    6
Total Columns:    11

── NULL / MISSING VALUES ──
✅ Employee_ID: 0 missing (0.0%)
🚨 Mob_No: 2 missing (33.3%)
🚨 Email_Address: 1 missing (16.7%)

🔍 Deduplication Results:
  Original records: 6
  Clean records:    5
  Duplicates found: 1

📊 Employee Headcount by Department:
Department  Employee_Count  Total_Salary  Avg_Salary
IT          2               170000        85000.0
HR          2               125000        62500.0
Finance     1               65000         65000.0

📊 Attrition Analysis Results:
  Total_Exited: 1
  Avg_Tenure_Days: 1439.0
  Avg_Tenure_Months: 48.0
```

