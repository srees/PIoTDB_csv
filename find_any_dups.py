import csv

def find_duplicate_rows(csv_file_path):
    dups = 0
    first_dup = ""
    seen_stamps = {}  # Dictionary to keep track of seen rows and their first occurrence
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row_number, current_row in enumerate(reader, start=1):
            stamp = current_row[0]  # Convert the row to a tuple for hashing
            if stamp in seen_stamps:
                first_occurrence = seen_stamps[stamp]
                first_dup = f"First duplicate found at row {row_number}, first occurred at row {first_occurrence}"
                dups += 1
            else:
                seen_stamps[stamp] = row_number  # Store the row and its line number
    print(f"{dups} duplicates found.")
    print(f"{first_dup}")

if __name__ == "__main__":
    # Prompt for the CSV file path
    csv_file_path = input("Enter the path to the CSV file: ")
    find_duplicate_rows(csv_file_path)
