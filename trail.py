import os
import pandas as pd
import zipfile
from io import BytesIO

# --- 1. Define Folders ---
root_folder = r"p:\helloworld\01pingbix\poorvika\DND\trail\paste_zip_folders_here"
# Define a separate output folder (one level up)
parent_dir = os.path.dirname(root_folder)
output_folder = os.path.join(parent_dir, "Summary_Output")

summary_data = []
output_filename = "NCPR_Fail_Summary.xlsx" # Just the file name

# --- Helper function to get all required stats ---
def get_stats_from_dataframe(df, file_name_str):
    """
    Analyzes a single DataFrame and returns all required stats.
    Returns (0, 0, 0, 0, set()) if columns are missing.
    """
    df.columns = [c.strip() for c in df.columns]
    
    # Check for all required columns
    required_cols = ["Status", "Cause", "SenderId"]
    if not all(col in df.columns for col in required_cols):
        print(f"   Skipped inner file {file_name_str}: Missing one or more required columns (Status, Cause, SenderId).")
        return 0, 0, 0, 0, set() # Return empty stats

    # --- Calculate all stats ---
    total_rows = len(df)
    
    # Total Delivered
    delivered_count = (df["Status"].astype(str).str.upper() == "DELIVERED").sum()
    
    # Total Failed (anything not delivered)
    failed_count = total_rows - delivered_count
    
    # NCPR Failed (a subset of 'total failed')
    ncpr_fail_count = (df["Cause"].astype(str).str.upper() == "NCPR FAIL").sum()
    
    # Get all unique SenderIDs from this file
    sender_ids = set(df["SenderId"].astype(str).unique())
    
    return total_rows, delivered_count, failed_count, ncpr_fail_count, sender_ids

# --- Loop through files directly in the root folder ---
print(f"Processing data files in: {root_folder}")

for file_name in os.listdir(root_folder):
    file_path = os.path.join(root_folder, file_name)
    
    # Get the date from the file name
    date_str = os.path.splitext(file_name)[0]

    # Case 1: Normal Excel file (not a ZIP)
    if file_name.endswith(".xlsx") and os.path.isfile(file_path):
        if file_name == output_filename:
            print(f"   Skipping old summary file: {file_name}")
            continue
            
        print(f"   → Reading Excel: {file_name}")
        try:
            df = pd.read_excel(file_path)
            # Get stats for this single file
            stats = get_stats_from_dataframe(df, file_name)
            total_rows, delivered_count, failed_count, ncpr_fail_count, sender_ids = stats

            if total_rows > 0: # Only add if valid
                summary_data.append({
                    "date": date_str,
                    "SENDERNAME": ", ".join(sender_ids), # Join the set
                    "total sent": total_rows,
                    "total deliveried": delivered_count,
                    "total failed": failed_count,
                    "ncpr failed": ncpr_fail_count
                })
                print(f"   Successfully processed: {file_name}")

        except Exception as e:
            print(f"   Error reading {file_name}: {e}")

    # Case 2: ZIP file (AGGREGATION LOGIC)
    elif file_name.endswith(".zip") and os.path.isfile(file_path):
        print(f"   → Extracting ZIP: {file_name}")
        
        # --- Initialize counters for the whole ZIP ---
        zip_total_rows = 0
        zip_delivered_count = 0
        zip_failed_count = 0
        zip_ncpr_fail_count = 0
        zip_sender_ids = set() # Use a set to store unique IDs from all inner files
        
        try:
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                for inner_name in zip_ref.namelist():
                    
                    df = None
                    try:
                        if inner_name.endswith(".xlsx"):
                            with zip_ref.open(inner_name) as inner_file:
                                df = pd.read_excel(inner_file)
                        
                        elif inner_name.endswith(".csv"):
                            with zip_ref.open(inner_name) as inner_file:
                                df = pd.read_csv(inner_file, on_bad_lines='skip')
                        
                        if df is not None:
                            # Get stats for this inner file
                            stats = get_stats_from_dataframe(df, inner_name)
                            inner_total, inner_deliv, inner_fail, inner_ncpr, inner_senders = stats
                            
                            # Add stats to the ZIP's total
                            zip_total_rows += inner_total
                            zip_delivered_count += inner_deliv
                            zip_failed_count += inner_fail
                            zip_ncpr_fail_count += inner_ncpr
                            zip_sender_ids.update(inner_senders) # Add new senders to the set
                        
                    except Exception as inner_e:
                        print(f"   Error reading inner file {inner_name}: {inner_e}")
            
            # --- After processing all inner files, add ONE row for the ZIP ---
            if zip_total_rows > 0:
                summary_data.append({
                    "date": date_str,
                    "SENDERNAME": ", ".join(zip_sender_ids), # Join all unique senders
                    "total sent": zip_total_rows,
                    "total deliveried": zip_delivered_count,
                    "total failed": zip_failed_count,
                    "ncpr failed": zip_ncpr_fail_count
                })
                print(f"   Successfully processed and aggregated: {file_name}")
            else:
                print(f"   No valid data found in ZIP: {file_name}")

        except Exception as e:
            print(f"   Error reading ZIP {file_name}: {e}")

# --- Create summary DataFrame ---
if summary_data:
    # --- Create DataFrame with the new columns ---
    summary_df = pd.DataFrame(summary_data)
    
    # Sort by date
    summary_df = summary_df.sort_values(by="date")
    
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Define the full output path in the new folder
    output_path = os.path.join(output_folder, output_filename)
    
    try:
        summary_df.to_excel(output_path, index=False)
        print(f"\nSummary saved successfully to: {output_path}")
    except PermissionError:
        print(f"\n--- FAILED TO SAVE ---")
        print(f"Permission denied: {output_path}")
        print(f"**Please close the Excel file before running the script.**")
    except Exception as e:
        print(f"\nAn error occurred while saving: {e}")
        
else:
    print("\nNo data was successfully processed.")