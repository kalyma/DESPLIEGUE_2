import psycopg2
conn = psycopg2.connect(
    host="dpg-d1a36mje5dus73e6har0-a.virginia-postgres.render.com",
    dbname="antoecom_skool",
    user="sa",
    password="EJaCIWj2fqYX5W3r5YyfDIGcQqqrtmEe"
)
print("¡Conexión exitosa!")