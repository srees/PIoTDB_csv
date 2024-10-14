from iotdb.Session import Session

def count_records_for_date(session, experiment, date, measurement):
    # Prepare the SQL query using the date parameter
    query = f"SELECT COUNT({measurement}) FROM root.{experiment}.*.{date}.*.can_bus_data"
    print(query)
    try:
        # Execute the query
        data_set = session.execute_query_statement(query)

        total_count = 0

        # Loop through the results and sum the counts
        columns = data_set.get_column_names()
        row = data_set.next()
        for i, columnname in enumerate(columns):
            total_count += row.get_fields()[i].get_long_value()  # Assume COUNT(*) returns a long integer

        print(f"Total record count for {date}: {total_count}")
        return total_count
    except Exception as e:
        print(f"Error querying IoTDB: {e}")

def main():
    # Connect to IoTDB
    ip = "localhost"
    port = "6667"
    user = "root"
    password = "root"

    experiment = input("Enter the experiment name: ")
    date = input("Enter the date of data: ")
    measurement = input("Enter the measurement name: ")

    session = Session(ip, port, user, password)
    session.open(False)


    # Query and count records for the specified date
    count_records_for_date(session, experiment, date, measurement)

    # Close the session
    session.close()

if __name__ == "__main__":
    main()
