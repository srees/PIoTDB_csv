import csv

def count_unique_first_column(csv_file_path):
    unique_entries = set()  # Set to store unique entries
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the first row (header)
        for row in reader:
            stamp = int(round(float(row[0]) * 1000000))
            unique_entries.add(f"{stamp}{row[-1]}")  # Add the first column entry to the set
    return len(unique_entries)

if __name__ == "__main__":
    csv_file_path = input("Enter the path to the CSV file: ")
    unique_count = count_unique_first_column(csv_file_path)
    print(f"Number of unique entries in the first column: {unique_count}")
