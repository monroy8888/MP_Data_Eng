from .queries import queries
from connection.mysql_connection import database


async def select_data(table_name):
    try:
        await database.connect()

        query = f"SELECT * FROM {table_name};"
        result = await database.fetch_all(query)

        table_content = []

        for row in result:
            row_dict = dict(row)
            table_content.append(row_dict)

        return table_content
    except Exception as e:
        print(f"Error al seleccionar datos de la tabla '{table_name}': {e}")
        return []
    finally:
        await database.disconnect()


async def select_query(query):
    try:
        await database.connect()

        query_apply = queries[query]
        result = await database.fetch_all(query_apply)

        table_content = []

        for row in result:
            row_dict = dict(row)
            table_content.append(row_dict)

        return table_content
    except Exception as e:
        print(f"Error al seleccionar datos de la tabla '{query}': {e}")
        return []
    finally:
        await database.disconnect()
