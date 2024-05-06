from pyhive import hive
import pandas as pd

hive_host = 'localhost'
hive_port = 10000
hive_database = 'mbds_g5'

conn = hive.Connection(host=hive_host, port=hive_port, database=hive_database)

cursor = conn.cursor()

cursor.execute('SHOW DATABASES')
databases = cursor.fetchall()
print(databases)

cursor.execute('SHOW TABLES')
tables = cursor.fetchall()
print(tables)

# get data
sql = '''SELECT * FROM catalogue'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df_catalogue = pd.DataFrame(rows)
    df_catalogue.columns = ['id', 'marque', 'nom', 'puissance', 'longueur', 'nbPlaces', 'nbPortes', 'couleur', 'occasion', 'prix']
    df_catalogue.to_csv('catalogue.csv', index=False)

sql = '''SELECT * FROM marketing'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df_marketing = pd.DataFrame(rows)
    df_marketing.columns = ['id', 'age', 'deuxiemevoiture', 'nbenfantsacharge', 'sexe', 'situationfamiliale', 'taux']
    df_marketing.to_csv('marketing.csv', index=False)


sql = '''SELECT * FROM co2'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df_co2 = pd.DataFrame(rows)
    df_co2.columns = ['id', 'marque', 'bonus_malus', 'rejets_co2', 'cout_energie']
    df_co2.to_csv('co2.csv', index=False)


sql = '''SELECT * FROM client'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df_client = pd.DataFrame(rows)
    df_client.columns = ['id', 'age', 'sexe', 'taux', 'situationfamiliale', 'nbenfantsacharge', 'deuxiemevoiture', 'immatriculation']
    df_client.to_csv('client.csv', index=False)


sql = '''SELECT * FROM immatriculation'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df_immatriculation = pd.DataFrame(rows)
    df_immatriculation.columns = ['id', 'immatriculation', 'nom', 'puissance', 'longueur', 'nbPlaces', 'nbPortes', 'couleur', 'occasion', 'prix']
    df_immatriculation.to_csv('immatriculation.csv', index=False)


sql = '''SELECT * FROM co2joincatalogue'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df_co2joincatalogue = pd.DataFrame(rows)
    df_co2joincatalogue.columns = ['id', 'marque', 'nom', 'puissance', 'longueur', 'nbPlaces', 'nbPortes', 'couleur', 'occasion', 'prix', 'bonus_malus', 'rejets_co2', 'cout_energie']
    df_co2joincatalogue.to_csv('co2joincatalogue.csv', index=False)


sql = '''SELECT * FROM immatriculationjoinclient'''
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print("No data found.")
    exit()
else:
    print("Data found.")
    df_immatriculationjoinclient = pd.DataFrame(rows)
    df_immatriculationjoinclient.columns = ['id', 'age', 'sexe', 'taux', 'situationfamiliale', 'nbenfantsacharge', 'deuxiemevoiture', 'immatriculation', 'nom', 'puissance', 'longueur', 'nbPlaces', 'nbPortes', 'couleur', 'occasion', 'prix']
    df_immatriculationjoinclient.to_csv('immatriculationjoinclient.csv', index=False)


cursor.close()
conn.close()