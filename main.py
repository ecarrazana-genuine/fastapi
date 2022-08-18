from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from azure_sql.azure_synapse import get_synapse_connection
from elastic.elastic_ds import ElasticQ
import uvicorn
from time import time

app = FastAPI()


@app.get("/")
def home():
    return {"message": "Hello from Genuine"}


@app.get("/test_azure_synapse")
def home():
    time_ini = time()
    db_conn = get_synapse_connection()
    query = "select count(*) from staging.eligibility"
    result = db_conn.execute(query)
    total_time = round((time() - time_ini) / 60, 2)
    return {"result": jsonable_encoder(result), "execution_time": total_time}


@app.get("/test_azure_elastic")
def elastic():
    time_ini = time()
    es = ElasticQ()
    query = {"match_all": {}}
    result = es.count("eligibility", query)
    total_time = round((time() - time_ini) / 60, 2)
    return {"result": jsonable_encoder(result), "execution_time": total_time}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=5000, log_level="info")
