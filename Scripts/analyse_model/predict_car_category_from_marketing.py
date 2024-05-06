import pandas as pd
from sqlalchemy import create_engine
import joblib
import pyodbc

# Load the trained model
loaded_model = joblib.load('car_category_prediction_model.pkl')

# Establish connection to Hive
hive_engine = create_engine('hive://vagrant@localhost:10000/mbds_g5')

# Execute SQL query to retrieve data from Hive table
sql_query_hive = """
    SELECT id, age, taux, nbenfantsacharge, sexe, situationfamiliale, deuxiemevoiture
    FROM marketing
"""

hive_df = pd.read_sql(sql_query_hive, hive_engine)

# Prepare an empty DataFrame to store predictions
predictions_df = pd.DataFrame(columns=['Prediction'])

# Iterate over each row in the Hive DataFrame
for index, row in hive_df.iterrows():
    input_data = pd.DataFrame({
        'age': [row['age']],
        'sexe_F': [0],
        'sexe_M': [0],
        'taux': [row['taux']],
        'situationfamiliale_Celibataire': [0],
        'situationfamiliale_En Couple': [0],
        'situationfamiliale_Marie(e)': [0],
        'situationfamiliale_Divorcee': [0],
        'nbenfantsacharge': [row['nbenfantsacharge']],
        'deuxiemevoiture_FALSE': [0],
        'deuxiemevoiture_TRUE': [0]
    })

    # Map input values to the corresponding columns
    input_data[f'sexe_{row["sexe"]}'] = 1
    input_data[f'situationfamiliale_{row["situationfamiliale"]}'] = 1
    input_data[f'deuxiemevoiture_{row["deuxiemevoiture"]}'] = 1

    input_data = input_data[['age', 'taux', 'nbenfantsacharge', 'sexe_F', 'sexe_M', 'situationfamiliale_Celibataire',
                             'situationfamiliale_Divorcee', 'situationfamiliale_En Couple', 'situationfamiliale_Marie(e)',
                             'deuxiemevoiture_FALSE', 'deuxiemevoiture_TRUE']]

    # Perform prediction
    X_hive = pd.get_dummies(input_data)  # One-hot encode categorical variables
    prediction = loaded_model.predict(X_hive)
    
    # Append the prediction to the DataFrame
    predictions_df.loc[index] = prediction[0]

# Concatenate the prediction column with the hive_df DataFrame
hive_df_with_predictions = pd.concat([hive_df, predictions_df], axis=1)

print(hive_df_with_predictions)

# Establish connection to SQL Server database
server = '192.168.11.122'
database = 'AUTOMOBILE'
username = 'sa'
password = 'sa'
# Create a connection string
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Establish a connection to SQL Server
conn = pyodbc.connect(conn_str)

# Create a cursor object
cursor = conn.cursor()

# Define table creation query
create_table_query = """
CREATE TABLE newClientPrediction (
    id INT PRIMARY KEY,
    age INT,
    taux INT,
    nbenfantsacharge INT,
    sexe VARCHAR(10),
    situationfamiliale VARCHAR(20),
    deuxiemevoiture VARCHAR(5),
    Prediction VARCHAR(20)
)
"""

# Execute the create table query
cursor.execute(create_table_query)
conn.commit()

# Iterate over each row in the DataFrame and insert it into the SQL Server table
for index, row in hive_df_with_predictions.iterrows():
    cursor.execute('''
    INSERT INTO newClientPrediction VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', tuple(row))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Data has been successfully inserted into the SQL Server table.")
