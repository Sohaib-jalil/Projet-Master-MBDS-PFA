# Import necessary libraries
import pyodbc
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler

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

print(client_data)


# Select features and target variable
X = client_data[['age', 'nbenfantsacharge', 'taux', 'situationfamiliale', 'sexe', 'deuxiemevoiture']]
y = client_data['Categorie']

# Convert categorical variables to numerical labels
X = pd.get_dummies(X)

# Normalize the features
scaler = StandardScaler()
X = scaler.fit_transform(X)

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=69)

# Initialize and train a K-Nearest Neighbors (KNN) Classifier
knn_classifier = KNeighborsClassifier(n_neighbors=5)
knn_classifier.fit(X_train, y_train)

# Predict vehicle categories for the test set
y_pred = knn_classifier.predict(X_test)

# Calculate accuracy score
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)