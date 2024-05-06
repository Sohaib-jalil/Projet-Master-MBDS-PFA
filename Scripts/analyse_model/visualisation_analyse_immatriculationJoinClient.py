from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
from sklearn.preprocessing import LabelEncoder

hive_host = 'localhost'
hive_port = 10000
hive_database = 'mbds_g5'

conn = hive.Connection(host=hive_host, port=hive_port, database=hive_database)

cursor = conn.cursor()

# get data
sql = '''SELECT * FROM immatriculationJoinClient'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df = pd.DataFrame(rows)
    df.columns = ['id', 'age', 'sexe', 'taux', 'situationfamiliale', 'nbenfantsacharge', 'deuxiemevoiture', 'immatriculation', 'nom', 'puissance', 'longueur', 'nbPlaces', 'nbPortes', 'couleur', 'occasion', 'prix']
    print( df.head())


# Histogramme de la puissance
plt.hist(df['puissance'], bins=6, color='skyblue', edgecolor='black')
plt.title('Histogramme de la puissance')
plt.xlabel('Puissance')
plt.ylabel('Fréquence')
plt.show()

# Nuage de points de la puissance par rapport au prix
plt.scatter(df['puissance'], df['prix'], color='skyblue')
plt.title('Nuage de points : Puissance vs Prix')
plt.xlabel('Puissance')
plt.ylabel('Prix')
plt.show()


# Boîte à moustaches de la puissance
sns.boxplot(x=df['occasion'], y=df['taux'])
plt.title('Boîte à moustaches : occasion vs taux')
plt.xlabel('occasion')
plt.ylabel('taux')
plt.show()


cursor.close()
conn.close()