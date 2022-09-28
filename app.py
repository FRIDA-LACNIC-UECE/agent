import os

from flask import request
from flask_migrate import Migrate
from sqlalchemy.orm import Session

from controller import (
    app, db, create_model_controller, database_controller, sse_contoller
)


Migrate(app, db)

@app.route('/')
def index():
    return {'message': "Agent is running."}, 200

if __name__ == "__main__":
    app.run(debug=True, port=5000)