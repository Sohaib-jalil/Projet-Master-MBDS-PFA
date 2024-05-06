# Import necessary libraries
import pyodbc
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score
import joblib

# Define your SQL Server connection parameters
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

# Execute SQL query to retrieve client data with the new "Category" variable
sql_query = """
        SELECT age
            ,sexe
            ,taux
            ,situationfamiliale
            ,nbenfantsacharge
            ,deuxiemevoiture
            ,Categorie
        FROM immatriculationJoinClientCategory
    """

# Load the client data with the new "Category" variable
client_data = pd.read_sql(sql_query, conn)

# Separate features (X) and target variable (y)
X = client_data[['age', 'sexe', 'taux', 'situationfamiliale', 'nbenfantsacharge', 'deuxiemevoiture']]
y = client_data['Categorie']

# One-hot encode categorical variables
X = pd.get_dummies(X)

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=69)

# Initialize and train a Gradient Boosting classifier
gb_classifier = GradientBoostingClassifier()
gb_classifier.fit(X_train, y_train)

# Get feature names in the same order as they appear in the DataFrame columns
feature_names = X.columns.tolist()
print("feature_names: ", feature_names)

# Predict vehicle categories for the test set
y_pred = gb_classifier.predict(X_test)

# Calculate accuracy score
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)

# Save the trained model to a file
joblib.dump(gb_classifier, 'car_category_prediction_model.pkl')
