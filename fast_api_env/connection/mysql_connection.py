import sqlalchemy
import databases
from dotenv import dotenv_values

DATABASE_URL = f"postgresql://myuser:mypassword@postgres-db:5432/mercadoPago"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

engine = sqlalchemy.create_engine(DATABASE_URL)

metadata.create_all(engine)
