# --- START OF FILE clean_data.py (Version 3 - Final and Correct) ---

import os
import argparse

def clean_csv_file(filepath, dry_run=False):
    """
    Checks a CSV file for duplicated data blocks and truncates it if corruption is found.
    It identifies the start of a data block by looking for the 9:15 AM timestamp.

    Args:
        filepath (str): The full path to the CSV file.
        dry_run (bool): If True, only reports what it would do without changing files.

    Returns:
        bool: True if the file was fixed (or would be fixed in dry run), False otherwise.
    """
    try:
        with open(filepath, 'r', errors='ignore') as f:
            lines = f.readlines()

        # A valid file must have at least a header and one line of data.
        if len(lines) <= 1:
            return False

        # --- THIS IS THE NEW, CORRECT CHECK ---
        # The marker for the start of a data block is the 9:15 AM timestamp.
        # The leading space is important to avoid matching times like '10:09:15'.
        data_start_marker = " 09:15:00"
        
        # Find all line numbers where a new data block seems to start.
        marker_indices = [i for i, line in enumerate(lines) if data_start_marker in line]
        # --- END OF NEW CHECK ---

        # If the marker appears once or not at all, the file is likely clean.
        if len(marker_indices) <= 1:
            return False

        # --- Corruption Detected ---
        
        # The truncation point is the line number of the second occurrence of the marker.
        truncation_point = marker_indices[1]
        
        # The "good" data is everything before this point.
        good_lines = lines[:truncation_point]
        
        original_line_count = len(lines)
        cleaned_line_count = len(good_lines)

        if dry_run:
            print(f"[DRY RUN] Corrupted file found: {filepath}")
            print(f"          - Would truncate from {original_line_count} to {cleaned_line_count} lines.")
        else:
            # Overwrite the file with the cleaned content.
            with open(filepath, 'w') as f:
                f.writelines(good_lines)
            print(f"[FIXED] Truncated corrupted file: {filepath}")
            print(f"        - Original lines: {original_line_count}, Cleaned lines: {cleaned_line_count}")
            
        return True

    except Exception as e:
        print(f"[ERROR] Could not process file {filepath}: {e}")
        return False

def main():
    """Main function to orchestrate the data cleaning process."""
    parser = argparse.ArgumentParser(
        description="A utility to find and fix corrupted (duplicated) options data files downloaded by Sakshi.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "data_directory",
        help="The root data directory to scan (e.g., '.' for current, or 'nifty')."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan for corrupted files and report them without actually modifying any files."
    )
    args = parser.parse_args()

    root_dir = args.data_directory
    if not os.path.isdir(root_dir):
        print(f"❌ Error: Directory not found: '{root_dir}'")
        return

    print("--- Starting Data Cleaning Utility (v3 - Data Block Check) ---")
    if args.dry_run:
        print("⚠️  DRY RUN MODE IS ACTIVE. No files will be changed.")
    print(f"Scanning directory: {os.path.abspath(root_dir)}\n")

    files_scanned = 0
    files_fixed = 0

    for dirpath, dirnames, filenames in os.walk(root_dir):
        if "options_1s" in dirpath:
            for filename in filenames:
                if filename.endswith('.csv'):
                    files_scanned += 1
                    full_filepath = os.path.join(dirpath, filename)
                    
                    if clean_csv_file(full_filepath, dry_run=args.dry_run):
                        files_fixed += 1

    print("\n--- Scan Complete ---")
    print(f"Total CSV files scanned: {files_scanned}")
    if args.dry_run:
        print(f"Corrupted files found: {files_fixed}")
        if files_fixed > 0:
            print("\nRun the script again without the --dry-run flag to fix these files.")
    else:
        print(f"Corrupted files fixed: {files_fixed}")
    print("---------------------\n")


if __name__ == "__main__":
    main()