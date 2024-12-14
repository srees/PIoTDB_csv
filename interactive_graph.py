# make sure you have pip installed/updated dash, scipy, numpy, apache-iotdb...
import dash
from dash import dcc, html, Input, Output, State
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from iotdb.Session import Session
from scipy.spatial import cKDTree
from math import radians, sin, cos, sqrt, atan2
import bisect

#############
# variables
#############
host=""
port=""
user=""
password=""
startTime = '07:00:00'
endTime = '08:59:59'
chunks = '30s'

def do_query(session, query):
    #t1 = time.perf_counter()
    print(query)
    try:
        data_set = session.execute_query_statement(query)
        #t2 = time.perf_counter()
        df = data_set.todf()
        #t3 = time.perf_counter()
        #print(len(df),'records in',t2-t1,'seconds', 'dataframe conversion in',t3-t2, 'seconds')
        return df
    except Exception as e:
        print(f"Error querying IoTDB: {e}")

def spatial_interpolation2(df):
    # We will generate our own Bearing column since atan2 is available in Python
    # Helper function to calculate the bearing between two points

    # Convert degrees to radians
    #    lat1, lon1, lat2, lon2 = map(np.radians, [df['Lat1'], df['Lon1'], df['Lat2'], df['Lon2']])

    # Compute the bearing
    #    dlon = lon2 - lon1
    #    x = np.sin(dlon) * np.cos(lat2)
    #    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    #    initial_bearing = np.arctan2(x, y)

    # Convert the bearing from radians to degrees and normalize it
    #    bearing = np.degrees(initial_bearing)
    #    df['Bearing'] = (bearing + 360) % 360  # Normalize to 0-360 range
    df['Bearing'] = calculate_bearing(df['Lat1'], df['Lon1'], df['Lat2'], df['Lon2'])
    df['Lat'] = (df['Lat1'] + df['Lat2']) / 2
    df['Long'] = (df['Lon1'] + df['Lon2']) / 2

    # Load mile marker data
    pm_df = pd.read_csv('link_mm_locations.csv')
    pm_df = pm_df[pm_df['road_side'] == 'w'].copy()

    # Drop rows with missing Lat, Long, Bearing
    df = df.dropna(subset=['Lat', 'Long', 'Bearing']).copy()

    # Convert Lat/Long to radians for calculations
    query_points = np.radians(df[['Lat', 'Long']].to_numpy())
    ref_points = np.radians(pm_df[['latitude', 'longitude']].to_numpy())

    # Create KDTree for fast neighbor searches
    tree = cKDTree(ref_points)

    # Query the 4 nearest neighbors
    k = 4
    distances, indices = tree.query(query_points, k=k)

    # Get the coordinates of the 4 nearest neighbors
    nearest_coords = ref_points[indices]  # Shape: (query_points, 4 neighbors, 2 coordinates)

    # Compute bearings for the 4 nearest neighbors
    lat1 = query_points[:, 0]  # Query latitudes
    lon1 = query_points[:, 1]  # Query longitudes
    lat2 = nearest_coords[:, :, 0]  # Nearest neighbor latitudes
    lon2 = nearest_coords[:, :, 1]  # Nearest neighbor longitudes

    # Compute the relative bearings
    delta_lon = lon2 - lon1[:, None]
    x = np.sin(delta_lon) * np.cos(lat2)
    y = np.cos(lat1[:, None]) * np.sin(lat2) - np.sin(lat1[:, None]) * np.cos(lat2) * np.cos(delta_lon)
    bearings = (np.degrees(np.arctan2(x, y)) % 360)  # Shape: (query_points, 4 neighbors)

    # Compute relative bearings (adjusted to vehicle bearing)
    relative_bearings = (bearings - df['Bearing'].values[:, None] + 360) % 360

    # Classify neighbors as left or right
    left_mask = (relative_bearings > 90) & (relative_bearings < 270)  # Left points
    right_mask = ~left_mask  # Right points

    # Initialize placeholders for the closest left and right neighbors
    left_indices = np.full(len(df), -1)
    right_indices = np.full(len(df), -1)
    left_distances = np.full(len(df), np.inf)
    right_distances = np.full(len(df), np.inf)

    # Find the closest left and right neighbors
    for i in range(k):
        is_left = left_mask[:, i]
        is_right = right_mask[:, i]

        closer_left = is_left & (distances[:, i] < left_distances)
        closer_right = is_right & (distances[:, i] < right_distances)

        left_indices[closer_left] = indices[closer_left, i]
        left_distances[closer_left] = distances[closer_left, i]

        right_indices[closer_right] = indices[closer_right, i]
        right_distances[closer_right] = distances[closer_right, i]

    # Assign left and right markers
    left_markers = np.full(len(df), np.nan)
    right_markers = np.full(len(df), np.nan)
    valid_left = left_indices != -1
    valid_right = right_indices != -1

    if valid_left.any():
        left_markers[valid_left] = pm_df.iloc[left_indices[valid_left]]['calculated_mm'].values
    if valid_right.any():
        right_markers[valid_right] = pm_df.iloc[right_indices[valid_right]]['calculated_mm'].values

    # Convert to miles
    left_distances = left_distances * 3959
    right_distances = right_distances * 3959

    # Interpolate mile markers using distances
    total_distances = left_distances + right_distances
    interpolated_markers = (left_markers * right_distances + right_markers * left_distances) / total_distances

    # Calculate the projection distance along the road using the optimized formula
    # d_proj = (left_distances**2 + (right_markers-left_markers)**2 - right_distances**2) / (2 * (right_markers-left_markers))

    # Interpolate the mile marker position
    # interpolated_markers = left_markers + d_proj#(d_proj / (right_markers-left_markers)) * (right_markers - left_markers)

    # Assign the interpolated values to the DataFrame
    df['Mile Marker'] = interpolated_markers
    df['Left Mile Marker'] = left_markers
    df['Right Mile Marker'] = right_markers
    df['Left Distance'] = left_distances
    df['Right Distance'] = right_distances
    return df

# Helper function to calculate the bearing between two points
def calculate_bearing(lat1, lon1, lat2, lon2):
    # Convert degrees to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    # Compute the bearing
    dlon = lon2 - lon1
    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    initial_bearing = np.arctan2(x, y)

    # Convert the bearing from radians to degrees and normalize it
    bearing = np.degrees(initial_bearing)
    bearing = (bearing + 360) % 360  # Normalize to 0-360 range

    return bearing

def filter_data(df, NaN=False):
    ### Filtering Options ###
    # ** Note! **
    # Filtering vs. replacing with NaN affect the plot.
    # NaN causes breaks in the plot line whereas filter does not.

    ###################################
    # Filter to Westbound and MM Proximity
    ###################################
    if (NaN):
        df.loc[((df['Bearing'] < 225) & (df['Bearing'] > 1)), ['Mile Marker', 'Speed']] = np.nan
    else:
        df = df[(df['Bearing'] > 225) | (df['Bearing'] < 1)]
    # df.loc[(df['Distance to Mile Marker'] > 1.5) | (df['Bearing'] == 0), ['Mile Marker', 'Speed']] = np.nan

    ###################################
    # Filter to overlapping highway MM
    ###################################
    if (NaN):
        df.loc[(df['Mile Marker'] < 60.5) | (df['Mile Marker'] > 64.3), ['Speed', 'Mile Marker']] = np.nan
    else:
        df = df[(df['Mile Marker'] > 60.5) & (df['Mile Marker'] < 64.3)]
    ####

    ###################################
    # Filter by on/off ramps
    ###################################
    # Our data has cars using to separate sets of on/off ramps.
    # Compute max and min mile markers for each device
    # device_stats = df.groupby('Device')['Mile Marker'].agg(['min', 'max'])
    # Identify devices where the range is outside the specified range
    # valid_devices = device_stats[(device_stats['max'] > 65) & (device_stats['min'] > 59)].index
    # Filter the original dataframe to keep only valid devices
    # df = df[df['Device'].isin(valid_devices)]

    # Remove filter columns we no longer need
    # del df['Bearing']
    # del df['Lat']
    # del df['Long']
    # del df['Distance to Mile Marker']
    return df

# Initialize the app
app = dash.Dash(__name__)

# Initial Parameters
initial_date='2022-11-17'

# Layout
app.layout = html.Div([
    html.Div([
        html.Label("Enter Date:"),
        dcc.Input(id="date-input", type="text", value=initial_date),
        html.Button("Update", id="update-button"),
    ]),
    dcc.Graph(id="scatter-plot", config={"scrollZoom": True}),
])

# Callback for fetching data and updating the graph
@app.callback(
    Output("scatter-plot", "figure"),
    [Input("update-button", "n_clicks"),
     Input("scatter-plot", "relayoutData")],
    [State("date-input", "value")],
)
def update_graph(n_clicks, relayout_data, date_input):
    global startTime, endTime, chunks
    session = Session(host, port, user, password)
    session.open(False)

    # Parse the user date
    try:
        date = datetime.strptime(date_input, "%Y-%m-%d").date()
    except ValueError:
        date = "2022-11-17"

    print(date)
    datenode = date_input.replace('-', '_')
    align = ' align by device '
    vehicle = '*'

    # Adjust time range based on relayout data (pan/zoom)
    if relayout_data and "xaxis.range[0]" in relayout_data:
        passedStart = datetime.fromisoformat(relayout_data["xaxis.range[0]"])
        passedEnd = datetime.fromisoformat(relayout_data["xaxis.range[1]"])
        startTime = passedStart.time()
        endTime = passedEnd.time()
        time_difference = (passedEnd - passedStart).total_seconds()
        chunks = str(int(round(time_difference / 240, 0))) + 's'

    window = f'[{date}T{startTime},{date}T{endTime})'
    groupby = f' group by ({window}, {chunks}) '

    # Fetch data
    query = f'''
    SELECT 
        first_value(gps_data_Lat) as Lat1,
        last_value(gps_data_Lat) as Lat2, 
        first_value(gps_data_Long) as Lon1,
        last_value(gps_data_Long) as Lon2,
        avg(speed_speed)*0.621371 as Speed
    FROM root.circles100.*.{datenode}.*
    {groupby}{align}
    '''

    df = do_query(session, query)
    df = spatial_interpolation2(df)
    data = filter_data(df, NaN=True)
    data['Time'] = pd.to_datetime(data['Time'], unit='us') - timedelta(hours=6)
    #data = fetch_data(start_time, end_time, SAMPLE_SIZE)

    # Create scatter plot
    fig = px.scatter(
        data,
        x="Time",
        y="Mile Marker",
        color="Speed",
        color_continuous_scale="Viridis",
        title="Position vs Time (Colorized by Speed)",
    )
    fig.update_traces(mode="lines+markers")  # Add line connecting the points
    fig.update_layout(xaxis=dict(
        title="Time",
        type="date",  # Ensures that the axis is treated as datetime
        ),
        yaxis=dict(
            title="Position"
        ),
        transition_duration=500)  # Smooth transitions

    return fig

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
