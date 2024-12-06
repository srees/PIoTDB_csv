import dash
from dash import dcc, html, Input, Output
import pandas as pd
import plotly.graph_objects as go

# Sample database-like function
def fetch_data(start, end):
    """Simulate fetching data from a database between start and end times."""
    df = pd.DataFrame({
        'time': pd.date_range(start=start, end=end, freq='T'),
        'value': range(len(pd.date_range(start=start, end=end, freq='T')))
    })
    return df

# Initial data
initial_start = '2023-01-01 00:00:00'
initial_end = '2023-01-01 01:00:00'
df = fetch_data(initial_start, initial_end)

# Create the Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Graph(
        id='interactive-plot',
        config={'scrollZoom': True}  # Enable zooming and panning
    ),
    html.Div(id='range-display')  # Display the current range for debugging
])

# Initial figure setup
@app.callback(
    Output('interactive-plot', 'figure'),
    Input('interactive-plot', 'relayoutData')
)
def update_plot(relayout_data):
    """Update the plot dynamically based on zoom/pan range."""
    nonlocal initial_start, initial_end

    if relayout_data and 'xaxis.range[0]' in relayout_data:
        # Extract new range from relayout data
        new_start = relayout_data['xaxis.range[0]']
        new_end = relayout_data['xaxis.range[1]']
    else:
        # Use initial range if no relayout
        new_start, new_end = initial_start, initial_end

    # Fetch new data from the simulated database
    updated_df = fetch_data(new_start, new_end)

    # Create the updated figure
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=updated_df['time'],
        y=updated_df['value'],
        mode='lines+markers',
        name='Sample Data'
    ))

    fig.update_layout(
        title="Interactive Time Series",
        xaxis_title="Time",
        yaxis_title="Value",
        dragmode='pan',
        xaxis=dict(range=[new_start, new_end])  # Set the new range
    )

    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
