from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import duckdb
from contextlib import asynccontextmanager

class QueryRequest(BaseModel):
    query_vector: List[float] 
    top_k: int = 5  

@asynccontextmanager
async def lifespan(app: FastAPI):
    """ Initialize the DuckDB connection on app startup and store in state. """
    db_connector = duckdb.connect("src/database/vector.db")
    db_connector.execute("INSTALL vss;")
    db_connector.execute("LOAD vss;")
    db_connector.execute("SET GLOBAL hnsw_enable_experimental_persistence = true;")

    app.state.db_connector = db_connector
    yield
    db_connector.close()


app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    """ Root endpoint for health check. """
    return {"message": "Connected to vector db"}

@app.post("/query")
async def query_db(request: QueryRequest):
    """ Handles vector similarity queries. """

    print("Received query request:", request.model_dump())

    try:
        query_embedding = request.query_vector
        top_k = request.top_k

        if len(query_embedding) != 384:
            raise HTTPException(status_code=400, detail="Embedding must be exactly 384 dimensions")

        # vector to SQL array
        query_embedding_str = ", ".join(map(str, query_embedding))  
        cast_query = f"CAST(ARRAY[{query_embedding_str}] AS FLOAT[384])"

        print(f"Running SQL Query with Vector of Length: {len(query_embedding)}")

        sql_query = f"""
        SELECT url, array_cosine_similarity(embedding, {cast_query}) AS similarity
        FROM embeddings
        ORDER BY similarity DESC
        LIMIT ?;
        """

        db_connector = duckdb.connect("src/database/vector.db")
        results = db_connector.execute(sql_query, (top_k,)).fetchall()

        print(f"Query Results: {results}")

        return {"results": [{"url": row[0]} for row in results]}


    except Exception as e:
        print(f"Error in query processing: {e}")
        raise HTTPException(status_code=400, detail=str(e))





if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
