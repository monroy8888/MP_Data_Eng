from fastapi import FastAPI
from .controllers.controllers import router
from .connection.mysql_connection import database

app = FastAPI()
app.include_router(router)

@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()