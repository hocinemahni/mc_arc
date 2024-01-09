import pandas as pd

# Chemin du fichier de trace
trace_file_path = 'IBMObjectStoreTrace011Part0/IBMObjectStoreTrace011Part0.csv'  #

# Lecture du fichier de trace
df = pd.read_csv(trace_file_path, sep=' ', header=None, names=["timestamp", "Request_Type", "filename", "size", "offset", "io_size"])

# Filtrer pour ne garder que les requêtes GET
df_get = df[df['Request_Type'] == 'REST.GET.OBJECT']

# Conversion du timestamp en secondes
df_get['timestamp'] /= 1000

df_get['size'] = df_get['size'].astype(int)
df_get['offset'] = df_get['offset'].fillna(0).astype(int)
df_get['io_size'] = df_get['io_size'].fillna(0).astype(int)
# Garder seulement les colonnes nécessaires pour ibmiostrace.csv
df_get_final = df_get[['timestamp', 'filename', 'offset', 'io_size']]

# Sauvegarde du fichier modifié pour les requêtes GET
output_get_file_path = 'ibmiostrace11.csv'
df_get_final.to_csv(output_get_file_path, index=False)

# Créer et sauvegarder un fichier avec 'file_name' et 'file_size', sans redondance
metadata_df = df_get[['filename', 'size']].drop_duplicates()
metadata_output_file_path = 'metadataibm11.csv'
metadata_df.to_csv(metadata_output_file_path, index=False)

output_get_file_path, metadata_output_file_path
