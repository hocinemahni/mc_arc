import pandas as pd

# Lire le fichier CSV
df = pd.read_csv('IBMObjectStoreTrace002Part0/IBMObjectStoreTrace002Part0.csv', sep=' ', header=None)

# Renommer les colonnes
df.rename(columns={0: 'Timestamp', 1: 'Request_Type', 2: 'Object_ID', 3: 'Object_Size', 4: 'Start_Offset', 5: 'End_Offset'}, inplace=True)

# Filtrer pour les requêtes GET
df_get = df[df['Request_Type'] == 'REST.GET.OBJECT'].copy()

# Conversion du timestamp
df_get.loc[:, 'Timestamp'] /= 1000

# Renommer les colonnes restantes
df_get.rename(columns={'Object_ID': 'File_Name', 'Start_Offset': 'Offset', 'End_Offset': 'IO_Size'}, inplace=True)

# Supprimer les colonnes non nécessaires
df_get = df_get[['Timestamp', 'File_Name', 'Offset', 'IO_Size']]

# Sauvegarde du fichier modifié
df_get.to_csv('ibmiostrace.csv', index=False)

# Créer un fichier avec 'file_name' et 'file_size'

if 'Object_Size' in df.columns:
    metadata_df = df[['Object_ID', 'Object_Size']].drop_duplicates()
    metadata_df.rename(columns={'Object_ID': 'File_Name'}, inplace=True)
    metadata_df.to_csv('metadataibm.csv', index=False)
else:
    print("La colonne 'Object_Size' n'existe pas dans le DataFrame original.")
