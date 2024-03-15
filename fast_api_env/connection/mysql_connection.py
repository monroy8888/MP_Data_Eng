import sqlalchemy
import databases
from dotenv import dotenv_values

config = dotenv_values(".env")

DB_USER = config.get("DB_USER")
DB_PASSWORD = config.get("DB_PASSWORD")
DB_HOST = config.get("DB_HOST")
DB_PORT = config.get("DB_PORT")
DB_NAME = config.get("DB_NAME")

DATABASE_URL = f"postgresql://myuser:mypassword@postgres-db:5432/mercadoPago"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

engine = sqlalchemy.create_engine(DATABASE_URL)

metadata.create_all(engine)
