import os
import csv
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

# Function to connect to IoTDB instance
def connect_iotdb(host='localhost', port=6667, user='root', password='root'):
    session = Session(host, port, user, password)
    session.open(False)
    return session

# Function to create a time series with the topic as part of the field name and the specified data type
def create_time_series(session, base_path, topic, field_name, field_type):
    time_series_path = f"{base_path}.{topic}_{field_name}"
    
    # Check if the time series already exists
    if not session.check_time_series_exists(time_series_path):
        # Map field_type to IoTDB type
        iotdb_type = map_field_type(field_type)
        session.create_time_series(time_series_path, iotdb_type, TSEncoding.PLAIN, Compressor.SNAPPY)
        print(f"Time series {time_series_path} created with {iotdb_type} type, PLAIN encoding, and SNAPPY compression.")
    #else:
    #    print(f"Time series {time_series_path} already exists.")

# Helper function to map user-specified types to IoTDB types
def map_field_type(field_type):
    iotdb_type_map = {
        "FLOAT": TSDataType.FLOAT,
        "DOUBLE": TSDataType.DOUBLE,
        "INT32": TSDataType.INT32,
        "TEXT": TSDataType.TEXT
    }
    return iotdb_type_map.get(field_type.upper(), TSDataType.TEXT)  # Default to FLOAT if no match

# Function to only create database if it doesn't exist
def create_storage_group_if_not_exists(session, group_path):
    query = f"SHOW STORAGE GROUP {group_path}"
    try:
        result = session.execute_query_statement(query)
        # If the query returns any rows, the storage group already exists.
        if not result.has_next():
            session.set_storage_group(group_path)
            print(f"Storage group {group_path} created.")
        else:
            print(f"Storage group {group_path} already exists.")
    except Exception as e:
        print(f"Error while checking/creating storage group: {e}")

# Function to insert CAN data into IoTDB using batch inserts
def insert_can_data(filepath, experiment, date, topic, field_names, field_types, batch_size=100):
    session = connect_iotdb()

    # Define the storage group path at the experiment level
    storage_group_path = f"root.{experiment}"
    create_storage_group_if_not_exists(session, storage_group_path)

    with open(filepath, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        headers = next(csvreader)  # Skip header row

        count = 0

        # Iterate through the CSV file
        for row in csvreader:
            timestamp = int(round(float(row[0])*1000000))  # Keep timestamp as float to preserve microseconds
            vehicle_num = row[-2]      # Second to last column as vehicle number
            run_number = row[-1][-1]   # Last character of last column as run number

            # Construct the base path for inserting data
            base_path = f"root.{experiment}.vehicle_{vehicle_num}.{date}.run_{run_number}.can_bus_data"
            
            # Add each field to the batch
            for i, field_name in enumerate(field_names):
                # Create time series if it doesn't exist
                create_time_series(session, base_path, topic, field_name, field_types[i])

                # Add path, timestamp, and value to batch
                path = f"{base_path}.{topic}_{field_name}"
                
                # Add measurements (field_name)
                measurement = f"{topic}_{field_name}"

                # Convert value to the correct type based on the field type
                value = convert_value(row[i + 1], field_types[i])
                
                # Add field type data
                d_type = map_field_type(field_types[i])

                session.insert_record(base_path, timestamp, [measurement], [d_type], [value])
                # print(f"Inserted record.")
                count += 1

    session.close()
    print(f"Total records inserted: {count}")

# Helper function to convert CSV values to the correct data type
def convert_value(value, field_type):
    try:
        if field_type.upper() == "FLOAT":
            return float(value)
        elif field_type.upper() == "DOUBLE":
            return float(value)
        elif field_type.upper() == "INT32":
            return int(value)
        elif field_type.upper() == "INT64":
            return int(value)
        elif field_type.upper() == "BOOLEAN":
            return value.lower() in ['true', '1']
        elif field_type.upper() == "TEXT":
            return str(value)
    except ValueError:
        return None  # Handle conversion errors gracefully by returning None

# Prompt for user inputs
def main():
    date = input("Enter the date (YYYY-MM-DD): ")
    topic = input("Enter the topic: ")
    experiment = input("Enter the experiment name: ")
    filepath = input("Enter the file path: ")
    field_names = input("Enter a space-separated list of field names: ").split()
    field_types = input("Enter a space-separated list of field types corresponding to the field names: ").split()
    batch_size = int(input("Enter the batch size (default 100): ") or 100)

    if len(field_names) != len(field_types):
        print("Error: The number of field names must match the number of field types.")
        return

    # Call function to insert data
    insert_can_data(filepath, experiment, date, topic, field_names, field_types, batch_size)

if __name__ == "__main__":
    main()
