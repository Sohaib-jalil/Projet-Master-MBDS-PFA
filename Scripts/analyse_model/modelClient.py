import pyodbc
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score


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
        FROM ClientImmatriculationCluster
    """

# Load the client data with the new "Category" variable
client_data = pd.read_sql(sql_query, conn)

# Select features and target variable
X = client_data[['age', 'sexe', 'taux', 'situationfamiliale', 'nbenfantsacharge', 'deuxiemevoiture']]
y = client_data['Categorie']

# Split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=69)

# Define preprocessing steps for numerical and categorical features
numerical_features = ['age', 'taux', 'nbenfantsacharge']
categorical_features = ['sexe', 'situationfamiliale', 'deuxiemevoiture']

# Define a pipeline for preprocessing numerical and categorical features
numerical_transformer = StandardScaler()
categorical_transformer = OneHotEncoder(handle_unknown='ignore')

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numerical_features),
        ('cat', categorical_transformer, categorical_features)
    ])

# Define a pipeline for the whole modeling process
pipeline = Pipeline(steps=[('preprocessor', preprocessor),
                           ('classifier', DecisionTreeClassifier())])

# Define hyperparameters to search
param_grid = {
    'classifier__criterion': ['gini', 'entropy'],
    'classifier__max_depth': [None, 10, 20, 30, 50],
    'classifier__min_samples_split': [2, 5, 10],
    'classifier__min_samples_leaf': [1, 2, 4]
}

# Perform grid search with cross-validation
grid_search = GridSearchCV(pipeline, param_grid, cv=5, scoring='accuracy')
grid_search.fit(X_train, y_train)

# Get the best model
best_model = grid_search.best_estimator_

# Predict vehicle categories for the test set using the best model
y_pred = best_model.predict(X_test)

# Calculate accuracy score
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy:", accuracy)
