import threading

from flask import jsonify

from controller import app
from service.verification_service import check_thread


@app.route("/initializeVerification", methods=["GET"])
def initializeVerification():
    try:
        # Run task on thread
        thread = threading.Thread(target=check_thread)
        thread.start()

        return jsonify({"message": "verification_initialized"}), 201
    except:
        return jsonify({"message": "verification_not_initialized"}), 400
