from lists.schema import Schema

# Completely destroys and recreates the sample keyspace for this app.

schema = Schema("liststest")
schema.create_keyspace()
schema.create_column_families()
schema.close()
