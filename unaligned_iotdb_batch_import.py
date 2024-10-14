# #########################################
# Variables
# #########################################

# IoTDB Server (defaults included)
host='localhost'
port=6667
user='root'
password='root'

# Email Notification (does not work with GMail!)
recipient_email = ""
sender_name = ""
sender_email = ""
smtp_server = ""
smtp_port = 465  # 465 for SSL
login = ""
password = ""  # Store this more securely!

# #########################################
# Imports
# #########################################
import csv
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import time
import subprocess
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr

# ##########################################
# Main
# ##########################################
def main():
    # Prompt for user inputs
    filepath = input("Enter the file path: ")
    experiment = input("Enter the experiment name: ")
    date = input("Enter the date for data (YYYY_MM_DD): ")
    topic = input("Enter the topic: ")
    field_names = input("Enter a space-separated list of measurement names: ").split()
    field_types = input("Enter a space-separated list of measurement types corresponding to the measurement names: ").split()
    batch_size = int(input("Enter the batch size (default 100): ") or 100)

    # Ensure we have a type for each measurement field
    if len(field_names) != len(field_types):
        print("Error: The number of measurement names must match the number of measurement types.")
        return

    # Get line count so we can track percentage of progress
    total_lines = count_lines_in_csv(filepath)

    # Track our execudtion time
    start_time = time.time()
  
    # Call function to insert data
    verify_count = insert_can_data(filepath, experiment, date, topic, field_names, field_types, total_lines, batch_size)

    # Time results
    finish_time = time.time()
    total_time = finish_time - start_time
    print(f"Total run time: {total_time} sec.")

    # Verification information for email notification
    verify = f"Verification results: CSV rows (includes header row): {total_lines} IoTDB rows: {verify_count}"

    # Email notification
    notify_completion(date, topic, experiment, filepath, field_names, verify)

# ###################################
# CSV Functions
# ###################################

# Count the lines in CSV using external function WC (faster than embedded counting)
def count_lines_in_csv(file_path):
    try:
        # Run the wc -l command
        result = subprocess.run(['wc', '-l', file_path], capture_output=True, text=True, check=True)
        # The output of wc -l contains the line count followed by the filename
        # Get the line count from the output
        line_count = int(result.stdout.split()[0])
        return line_count
      
    except subprocess.CalledProcessError as e:
        print(f"Error executing wc: {e}")
        return None

# Helper function to convert CSV values to the correct data type in Python
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
        # Handle conversion errors gracefully by returning None
        return None 

# ###################################
# IoTDB Ingestion
# ###################################

# Main function to insert CAN data into IoTDB using batch inserts
def insert_can_data(filepath, experiment, date, topic, field_names, field_types, total_lines, batch_size=100):
    session = connect_iotdb()

    # Define the storage group path at the experiment level
    storage_group_path = f"root.{experiment}"
    create_storage_group_if_not_exists(session, storage_group_path)

    # Lists to hold data for insertion of multiple rows
    paths = []
    timestamps = []
    values = []
    d_types = []
    measurements = []

    # Track overall batch progress
    batch_count = 0

    # Open and read the CSV file
    with open(filepath, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        # Skip header row
        headers = next(csvreader)

        # Timing data
        read_start = time.time()

        # Track which measurement to use for verification. There can be multiple in the CSV
        verify_measurement = ""

        # Simple cache to avoid unnecessary database calls in the script
        created_ts = set()

        # Iterate through the each row of the CSV file
        for row in csvreader:
          
            # IoTDB requires datetime or long. Our data tracks microseconds as a float, so we need to convert it.
            timestamp = int(round(float(row[0]) * 1000000))
          
            # Second to last column as vehicle number (custom for our data)
            vehicle_num = row[-2]
          
            # Last section of last column as run number (custom for our data)
            run_number = row[-1].split('_')[-1]

            # Construct the base path for inserting data
            base_path = f"root.{experiment}.vehicle_{vehicle_num}.{date}.run_{run_number}.can_bus_data"

            # Some of our data have multiple measurements. This script's use case is for unaligned data,
            # so we need to insert a row for each measurement

            # Used in progress indication
            num_fields = len(field_names)
          
            # Add each field to the batch
            for i, field_name in enumerate(field_names):
              
                # Create time series if it doesn't exist
                time_series_path = f"{base_path}.{topic}_{field_name}"
                if time_series_path not in created_ts:
                    create_time_series(session, time_series_path, field_types[i])
                    created_ts.add(time_series_path)

                # Add path and timestamp to batch
                path = f"{base_path}"
                paths.append(path)
                timestamps.append(timestamp)

                # Add measurement names (field_name)
                measurement = f"{topic}_{field_name}"
                measurements.append([measurement])
                if not verify_measurement:
                    verify_measurement = measurement

                # Convert value to the correct type based on the field type
                value = convert_value(row[i + 1], field_types[i])
                # Add the converted value to the batch
                values.append([value])

                # Add measurement field type data
                d_types.append([map_field_type(field_types[i])])

            # Check if batch size is reached
            if len(paths) >= batch_size:
              
                # Time data per batch
                read_end = time.time()

                # Insert the data
                session.insert_records(paths, timestamps, measurements, d_types, values)
                
                # Track insertion time per batch
                insert_end = time.time()
                read_time = read_end - read_start
                read_start = time.time()
                insert_time = insert_end - read_end

                # Output monitoring data for user
                print(f"Inserted batch {batch_count} of {len(paths)} records.")
                print(f"Ingest time: {read_time} sec. + Insert time: {insert_time} sec.")
                percentage = (batch_count + 1) * batch_size / total_lines * 100 / num_fields
                print(f"Percentage complete: {percentage} %")

                # Reset batch lists for next set
                paths, timestamps, values, d_types, measurements = [], [], [], [], []
                batch_count += 1

        # Insert remaining records when there aren't enough to fill a batch left
        if paths:
            # Do the insert (no need for monitoring this last since we are done)
            session.insert_records(paths, timestamps, measurements, d_types, values)
            # Let the user know we finished!
            print(f"Inserted final batch of {len(paths)} records.")

    # Verify how many records are in IoTDB that match the given parameters.
    # Default configuration in IoTDB does not allow duplicate measurements for a given timestamp.
    # So this ends up being rerun safe and still gives how many records are actually in the database.
    # We return this so it can go out in the email notification.
    verify_count = count_records_for_date(session, date, verify_measurement, experiment)

    # Clean up database connection
    session.close()

    # Monitoring info
    print(f"Total batches inserted: {batch_count}")
  
    return verify_count

# ##################################
# IoTDB Helper functions
# ##################################

# Verification routine
def count_records_for_date(session, date, measurement, experiment):
  
    # Prepare the SQL query using the date parameter
    query = f"SELECT COUNT({measurement}) FROM root.{experiment}.*.{date}.*.can_bus_data"

    try:
        # Execute the query
        data_set = session.execute_query_statement(query)

        total_count = 0

        # Because of the structure of IoTDB tineseries data, we get counts per timeseries and there is no 
        # function to get all matching timeseries together (ie. we have a timeseries per vehicle, per day, per run, per topic)
        # Loop through the results and sum the counts per tineseries
        columns = data_set.get_column_names()
        # There is only one row with our query result
        row = data_set.next()
        # There are many columns in our query result, one per timeseries
        for i, columnname in enumerate(columns):
            # Assume COUNT(*) returns a long integer
            total_count += row.get_fields()[i].get_long_value()
        # Print for user
        print(f"Total record count for {date}: {total_count}")
        # Return for email notification
        return total_count
      
    except Exception as e:
        print(f"Error querying IoTDB: {e}")

# Function to connect to IoTDB instance
def connect_iotdb():
    session = Session(host, port, user, password)
    session.open(False)
    return session

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

# Function to create a time series with the topic as part of the field name and the specified data type
def create_time_series(session, time_series_path, field_type):
    # Check if the time series already exists
    if not session.check_time_series_exists(time_series_path):
        # Map field_type to IoTDB type
        iotdb_type = map_field_type(field_type)
        session.create_time_series(time_series_path, iotdb_type, TSEncoding.PLAIN, Compressor.SNAPPY)
        print(f"Time series {time_series_path} created with {iotdb_type} type, PLAIN encoding, and SNAPPY compression.")

# Helper function to map user-specified types to IoTDB types
def map_field_type(field_type):
    iotdb_type_map = {
        "FLOAT": TSDataType.FLOAT,
        "DOUBLE": TSDataType.DOUBLE,
        "INT32": TSDataType.INT32,
        "TEXT": TSDataType.TEXT
    }
    return iotdb_type_map.get(field_type.upper(), TSDataType.TEXT)  # Default to FLOAT if no match

# ##################################
# Email/Notification Functions
# ##################################

# Call this function at the end of your script
def notify_completion(date, topic, experiment, filepath, field_names, verify):
    subject = "Script Completion Notification"
    body = f"Your script has finished running.\n\n{experiment}\n{date} {topic} {field_names}\n{filepath}\n{verify}"
    send_email(subject, body)

# email send function
def send_email(subject, body):
    # Create the email
    msg = MIMEMultipart()
    msg['From'] = formataddr((sender_name, sender_email))
    msg['To'] = recipient_email
    msg['Subject'] = subject

    # Attach the message body
    msg.attach(MIMEText(body, 'plain'))

    # Send the email
    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(login, password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

# ##################################
# ENTRY POINT
# ##################################
if __name__ == "__main__":
    main()
