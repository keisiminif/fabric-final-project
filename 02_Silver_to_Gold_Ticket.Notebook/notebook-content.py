# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "005aa4ad-f77a-4977-b6eb-cc3224e769c2",
# META       "default_lakehouse_name": "lh_final",
# META       "default_lakehouse_workspace_id": "b8038235-0da0-460c-bc4e-4f48b9797fed",
# META       "known_lakehouses": [
# META         {
# META           "id": "005aa4ad-f77a-4977-b6eb-cc3224e769c2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

print("1. Lettura dati Silver (Locali e Virtualizzati)...")
# Lettura diretta tramite il metastore di Spark (evita problemi di percorsi relativi)
df_ticket = spark.table("silver_ticket")

# Fabric mappa automaticamente la cartella dello shortcut 'dbo_1' come schema database
df_clienti = spark.table("dbo_1.silver_clienti")

print("2. Creazione Modello Gold (Incrocio Dati)...")
# Eseguiamo un Left Join per arricchire i ticket con le informazioni dei clienti.
# La funzione unisce automaticamente tutte le colonne dell'anagrafica al ticket.
df_gold = df_ticket.join(df_clienti, "ID_Cliente", "left")

print("3. Salvataggio in Zona Gold...")
df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_ticket_analytics")

print("--- TABELLA GOLD CREATA CON SUCCESSO ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
