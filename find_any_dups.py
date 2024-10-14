import csv

def find_duplicate_rows(csv_file_path):
    seen_rows = {}  # Dictionary to keep track of seen rows and their first occurrence
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row_number, current_row in enumerate(reader, start=1):
            row_tuple = tuple(current_row)  # Convert the row to a tuple for hashing
            if row_tuple in seen_rows:
                first_occurrence = seen_rows[row_tuple]
                print(f"Duplicate found at row {row_number}, first occurred at row {first_occurrence}")
            else:
                seen_rows[row_tuple] = row_number  # Store the row and its line number

if __name__ == "__main__":
    # Prompt for the CSV file path
    csv_file_path = input("Enter the path to the CSV file: ")
    find_duplicate_rows(csv_file_path)
