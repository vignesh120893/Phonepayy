# Step 1: Data Extraction
import os
import git

# Define the repository URL
repo_url = "https://github.com/PhonePe/pulse.git"
# Define the directory to clone the repository into
repo_dir = "phonepe_pulse"

# Clone the repository
if not os.path.exists(repo_dir):
    git.Repo.clone_from(repo_url, repo_dir)
    print("Repository cloned successfully.")
else:
    print("Repository already exists.")

# Step 2: Data Transformation
import pandas as pd
import json

# Define the path to the data
data_path = os.path.join(repo_dir, "data")

# Function to load and preprocess data
def load_and_preprocess_data(data_path):
    all_data = []

    for subdir, _, files in os.walk(data_path):
        for file in files:
            file_path = os.path.join(subdir, file)
            if file_path.endswith('.json'):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    # Example of extracting and transforming data
                    for record in data['data']:
                        record['file'] = file  # Adding filename for reference
                        all_data.append(record)
    
    df = pd.DataFrame(all_data)
    # Data cleaning and transformation steps here
    df.dropna(inplace=True)  # Example: Drop missing values
    return df

# Load and preprocess the data
df = load_and_preprocess_data(data_path)
print(df.head())

# Step 3: Database Insertion
import mysql.connector

# MySQL database credentials
db_config = {
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
    'database': 'phonepe_pulse'
}

# Connect to MySQL database
cnx = mysql.connector.connect(**db_config)
cursor = cnx.cursor()

# Create table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS pulse_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    field1 VARCHAR(255),
    field2 VARCHAR(255),
    field3 VARCHAR(255),
    filename VARCHAR(255)
);
"""
cursor.execute(create_table_query)

# Insert data into the table
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO pulse_data (field1, field2, field3, filename)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(insert_query, (row['field1'], row['field2'], row['field3'], row['file']))

# Commit and close connection
cnx.commit()
cursor.close()
cnx.close()

# Step 4: Dashboard Creation
import streamlit as st
import plotly.express as px
import pandas as pd
import mysql.connector

# MySQL database credentials
db_config = {
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
    'database': 'phonepe_pulse'
}

# Fetch data from MySQL database
def fetch_data_from_db():
    cnx = mysql.connector.connect(**db_config)
    query = "SELECT * FROM pulse_data"
    df = pd.read_sql(query, cnx)
    cnx.close()
    return df

# Load data
df = fetch_data_from_db()

# Streamlit app layout
st.title("PhonePe Pulse Data Visualization")

# Dropdown options
dropdown_options = df['field1'].unique().tolist()
selected_option = st.selectbox("Select an option", dropdown_options)

# Filter data based on selected option
filtered_data = df[df['field1'] == selected_option]

# Plotly visualization
fig = px.scatter_geo(filtered_data, lat='lat', lon='lon', hover_name='field1', color='field2',
                     title=f"Visualization for {selected_option}")

# Display plot
st.plotly_chart(fig)

# Add more visualizations and options as needed
