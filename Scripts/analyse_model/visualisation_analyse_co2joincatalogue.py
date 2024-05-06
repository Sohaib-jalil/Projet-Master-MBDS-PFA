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
sql = '''SELECT * FROM co2joincatalogue'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df = pd.DataFrame(rows)
    df.columns = ['id', 'marque', 'nom', 'puissance', 'longueur', 'nbplaces', 'nbportes', 'couleur', 'occasion', 'prix', 'bonus_malus', 'rejets_co2', 'cout_energie']
    print( df.head())


# Histogramme de la puissance
plt.hist(df['bonus_malus'], bins=6, color='skyblue', edgecolor='black')
plt.title('Histogramme de la bonus_malus')
plt.xlabel('bonus_malus')
plt.ylabel('Fréquence')
plt.show()

# Nuage de points de la puissance par rapport au prix
plt.scatter(df['cout_energie'], df['rejets_co2'], color='skyblue')
plt.title('Nuage de points : cout_energie vs rejets_co2')
plt.xlabel('cout_energie')
plt.ylabel('rejets_co2')
plt.show()


# Boîte à moustaches de la puissance
sns.boxplot(x=df['occasion'], y=df['bonus_malus'])
plt.title('Boîte à moustaches : occasion vs bonus_malus')
plt.xlabel('occasion')
plt.ylabel('bonus_malus')
plt.show()




cursor.close()
conn.close()