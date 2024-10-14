import csv

def find_duplicate_rows(csv_file_path):
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        previous_row = None
        for row_number, current_row in enumerate(reader, start=1):
            if previous_row is not None and row_number != 2:
                if int(round(float(current_row[0]) * 1000000)) == int(round(float(previous_row[0]) * 1000000)) and float(current_row[1]) == float(previous_row[1]) and float(current_row[2]) == float(previous_row[2]) and current_row[3] == previous_row[3] and current_row[4] == previous_row[4]:
                    print(f"Duplicate found at row {row_number}")
            previous_row = current_row

if __name__ == "__main__":
    # Prompt for the CSV file path
    csv_file_path = input("Enter the path to the CSV file: ")
    find_duplicate_rows(csv_file_path)
