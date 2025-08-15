import os
from scripts.read_data import read_csv, read_txt, read_xlsx
from scripts.cleaning.cleaning_functions import drop_nulls, trim_strings, normalize_case, remove_duplicates
from scripts.validation.validation_rules import validate_sales_data

RAW_PATH = "data/raw"
PROCESSED_PATH = "data/processed"
REPORT_PATH = "reports"

os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(REPORT_PATH, exist_ok=True)

# List all files in raw folder
files = os.listdir(RAW_PATH)

for f in files:
    print(f"\nProcessing file: {f}")
    
    # Read file based on extension
    if f.endswith(".csv"):
        df = read_csv(f)
        is_pyspark = True
    elif f.endswith(".txt"):
        df = read_txt(f)
        is_pyspark = True
    elif f.endswith(".xlsx"):
        df = read_xlsx(f)
        is_pyspark = False 
    else:
        print(f"Skipping unsupported file: {f}")
        continue

    # Cleaning
    if is_pyspark:
        df = drop_nulls(df)
        df = trim_strings(df)
        df = normalize_case(df)
        df = remove_duplicates(df)
    else:
        pass
        # pandas cleaning
        df.dropna(inplace=True)
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip().str.lower())
        df.drop_duplicates(inplace=True)

    # Validation
    if is_pyspark:
        report = validate_sales_data(df)
    else:
        report = {col: df[col].isnull().sum() for col in df.columns}
    print("Validation report:", report)

    # Save cleaned data
    out_file = os.path.join(PROCESSED_PATH, f.replace(".", "_cleaned."))
    
    if is_pyspark:
        # Spark: coalesce to single CSV file
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_file)
    else:
        # Pandas: single CSV
        df.to_csv(out_file + "csv", index=False)

    print(f"Saved cleaned file to: {out_file}")

    # Save validation report
    with open(os.path.join(REPORT_PATH, f.replace(".", "_report.txt")), "w") as rep:
        for k, v in report.items():
            rep.write(f"{k}: {v}\n")
