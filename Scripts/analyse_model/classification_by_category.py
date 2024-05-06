from pyhive import hive
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import pyodbc

hive_host = 'localhost'
hive_port = 10000
hive_database = 'mbds_g5'

conn = hive.Connection(host=hive_host, port=hive_port, database=hive_database)

cursor = conn.cursor()

sql = '''SELECT * FROM immatriculationJoinClient'''
cursor.execute(sql)
rows = cursor.fetchall()

# Convertir les résultats de la requête en DataFrame pandas
columns = ['id', 'age', 'sexe', 'taux', 'situationfamiliale', 'nbenfantsacharge', 'deuxiemevoiture', 'immatriculation', 'nom', 'puissance', 'longueur', 'nbPlaces', 'nbPortes', 'couleur', 'occasion', 'prix']
df = pd.DataFrame(rows, columns=columns)

# Supprimer les lignes avec des valeurs manquantes
df.dropna(inplace=True)

# Sélectionner les caractéristiques pertinentes pour le clustering
features = df[['puissance', 'nbPortes', 'prix']]


# Normalize the data
scaler = StandardScaler()
scaled_features = scaler.fit_transform(features)

# Test de différentes valeurs de K pour trouver la meilleure
inertia = []
for k in range(1, 11):
    kmeans = KMeans(n_clusters=k, random_state=69)
    kmeans.fit(scaled_features)
    inertia.append(kmeans.inertia_)

# Tracer la courbe d'inertie pour choisir le nombre optimal de clusters
plt.plot(range(1, 11), inertia, marker='o')
plt.xlabel('Nombre de clusters (K)')
plt.ylabel('Inertie')
plt.title('Courbe d\'inertie')
plt.show()

# Basé sur la courbe d'inertie, choisissez le nombre optimal de clusters
# Entraînez le modèle avec le nombre optimal de clusters
k_optimal = 5  # Remplacez ceci par le nombre optimal trouvé sur la courbe
kmeans = KMeans(n_clusters=k_optimal, random_state=69)
kmeans.fit(scaled_features)

# Ajoutez les étiquettes des clusters à notre DataFrame
df['cluster'] = kmeans.labels_

# Créer un dictionnaire associant les catégories de véhicules à chaque cluster
cluster_categories = {
    0: "routieres",
    1: "familiales",
    2: "sport",
    3: "compactes",
    4: "luxe",
    # Ajoutez d'autres associations de cluster à catégorie au besoin
}
# Ajouter une colonne 'Catégorie' dans le DataFrame avec les informations clients et immatriculations
df['Categorie'] = df['cluster'].map(cluster_categories)
# Afficher les premières lignes du DataFrame avec la nouvelle colonne 'Catégorie'
df.drop(columns=['cluster'], inplace=True)
print(df[['age', 'nbenfantsacharge', 'taux', 'nbPlaces', 'nbPortes', 'prix', 'Categorie']].head())

df.to_csv('immatriculationJoinClientCategory.csv', index=False)


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

# Define your SQL query to create a table
create_table_query = '''
CREATE TABLE immatriculationJoinClientCategory (
    id INT PRIMARY KEY,
    age INT,
    sexe VARCHAR(10),
    taux FLOAT,
    situationfamiliale VARCHAR(50),
    nbenfantsacharge INT,
    deuxiemevoiture VARCHAR(10),
    immatriculation VARCHAR(50),
    nom VARCHAR(50),
    puissance INT,
    longueur VARCHAR(50),
    nbPlaces INT,
    nbPortes INT,
    couleur VARCHAR(50),
    occasion BIT,
    prix FLOAT,
    Categorie VARCHAR(50)
)
'''

# Execute the create table query
cursor.execute(create_table_query)
conn.commit()

# Iterate over each row in the DataFrame and insert it into the SQL Server table
for index, row in df.iterrows():
    cursor.execute('''
    INSERT INTO immatriculationJoinClientCategory VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', tuple(row))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()

print("Data has been successfully inserted into the SQL Server table.")