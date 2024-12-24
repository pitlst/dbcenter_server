from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.mount("/static", StaticFiles(directory="frontend/build/static"), name="static")

@app.get("/")
def root():
    return {"message": "Hello World"}