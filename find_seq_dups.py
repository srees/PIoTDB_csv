import csv

def find_duplicate_rows(csv_file_path):
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        previous_row = None
        for row_number, current_row in enumerate(reader, start=1):
            if previous_row is not None and current_row == previous_row:
                print(f"Duplicate found at row {row_number}")
            previous_row = current_row

if __name__ == "__main__":
    # Prompt for the CSV file path
    csv_file_path = input("Enter the path to the CSV file: ")
    find_duplicate_rows(csv_file_path)
