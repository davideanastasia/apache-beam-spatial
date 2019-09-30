import eventlet

# ref: https://github.com/miguelgrinberg/Flask-SocketIO/issues/235
eventlet.monkey_patch()

import logging

logging.basicConfig(level=logging.DEBUG)

import json

from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

NAMESPACE = '/tdrive'
LOGGER = logging.getLogger(__name__)

app = Flask(__name__)
# ref: https://github.com/miguelgrinberg/Flask-SocketIO/issues/618
# remember to pass 'app' in the init of SocketIO, or it won't work...
socketio = SocketIO(app,
                    logger=True,
                    engineio_logger=True)


@app.route('/')
def index():
    return render_template("index.html")


@app.route('/insert', methods=['POST'])
def insert():
    if request.json is None:
        print('empty push')
    else:
        # LOGGER.info(request.json)
        if 'id' in request.json:
            socketio.emit('add',
                          json.dumps(request.json),
                          namespace=NAMESPACE,
                          broadcast=True)

    return "OK"


@app.route('/remove', methods=['POST'])
def remove():
    # LOGGER.info(request.json)
    if 'id' in request.json:
        socketio.emit('rem',
                      json.dumps(request.json),
                      namespace=NAMESPACE,
                      broadcast=True)

    return "OK"


@socketio.on('connect', namespace=NAMESPACE)
def test_connect():
    LOGGER.info('Client connected')
    emit('connect_response', {'data': 'Connected'})


@socketio.on('disconnect', namespace=NAMESPACE)
def test_disconnect():
    LOGGER.info('Client disconnected')


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', debug=False)
