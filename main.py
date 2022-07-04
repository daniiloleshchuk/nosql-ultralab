from dotenv import load_dotenv
from flask import Flask, request

from file_service import FileService

load_dotenv()
app = Flask(__name__)


@app.route('/', methods=['POST'])
def load_file():
    data = request.get_json()
    endpoint = data.get('endpoint')
    target = data.get('target')
    file_service = FileService(endpoint=endpoint, target=target)
    return file_service.process()


def main():
    app.run(port=5001)


if __name__ == '__main__':
    main()
