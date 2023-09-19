import pandas as pd

# Lire le fichier CSV en entrée
input_file = 'IBMObjectStoreTrace000Part0.csv'


df = pd.read_csv(input_file, delimiter=' ')

# Filtrer les lignes DELETE et HEAD
df = df[df['Operation'] != 'REST.DELETE.OBJECT' and 'REST.HEAD.OBJECT']

# Créer un DataFrame pour les métadonnées des objets (Object_ID et Size)
metadata_df = df[['Object_ID', 'Size']].drop_duplicates()

# Créer un DataFrame pour les IOs
ios_df = df[['Object_ID', 'Start_Offset', 'End_Offset', 'Operation']].copy()


# Convertir les colonnes Size, Start_Offset et End_Offset en entiers
metadata_df['Size'] = metadata_df['Size'].astype(int)
ios_df['Start_Offset'] = ios_df['Start_Offset'].astype(int)
ios_df['End_Offset'] = ios_df['End_Offset'].astype(int)

# Obtenir la première valeur de la colonne "Operation" pour chaque groupe d'Object_ID
first_operation = ios_df.groupby('Object_ID')['Operation'].transform('first')

# Remplacer les valeurs manquantes dans Start_Offset et End_Offset en fonction de l'Operation
ios_df['Start_Offset'] = ios_df.apply(lambda row: 0 if row['Operation'] != 'REST.COPY.OBJECT' and first_operation[row.name] != 'REST.COPY.OBJECT' else row['Start_Offset'], axis=1)
ios_df['End_Offset'] = ios_df.apply(lambda row: metadata_df[metadata_df['Object_ID'] == row['Object_ID']]['Size'].values[0] - 1 if row['Operation'] != 'REST.COPY.OBJECT' and first_operation[row.name] != 'REST.COPY.OBJECT' else row['End_Offset'], axis=1)

# Supprimer la colonne Operation des IOs
ios_df.drop(columns=['Operation'], inplace=True)

# Enregistrer les deux DataFrames en fichiers CSV
metadata_df.to_csv('metadata.csv', index=False)
ios_df.to_csv('ios.csv', index=False)
