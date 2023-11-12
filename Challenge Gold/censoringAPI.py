import re
import pandas as pd
import sqlite3
import os
import datetime

from flask import *
from flasgger import *
from werkzeug.utils import secure_filename

# ganti file path UPLOAD_CSV_FOLDER_PRE dan UPLOAD_CSV_FOLDER_POST sesuai dengan komputer masing-masing
# run api pake /docs

app = Flask(__name__)

app.json_encoder = LazyJSONEncoder
swagger_template = dict(
    info={
        "title": "Censor Abusive Words",
        "version": "1.0.0",
        "description": "Dokumentasi API untuk menghilangkan kata-kata kasar dari sebuah text",
    },
    host="127.0.0.1:5000/",
)

swagger_config = {
    "headers": [],
    "specs": [
        {
            "endpoint": "docs",
            "route": "/docs.json",
        }
    ],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs/",
}
swagger = Swagger(app, template=swagger_template, config=swagger_config)


UPLOAD_CSV_FOLDER_PRE = os.path.abspath(
    r"C:\Users\Darren Iskandar\Binar DSC-14 Bootcamp\Challenge Gold\Pre-Cleansed"
)
UPLOAD_CSV_FOLDER_POST = os.path.abspath(
    r"C:\Users\Darren Iskandar\Binar DSC-14 Bootcamp\Challenge Gold\Post-Cleansed"
)
app.config["UPLOAD_CSV_FOLDER_PRE"] = UPLOAD_CSV_FOLDER_PRE
app.config["UPLOAD_CSV_FOLDER_POST"] = UPLOAD_CSV_FOLDER_POST


ALLOWED_EXTENSIONS = {"csv"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


df_abusive = pd.read_csv("data/abusive.csv")
abusive_list = df_abusive["ABUSIVE"].tolist()

df_singkatan = pd.read_csv(
    "data/new_kamusalay.csv", header=None, names=["singkatan", "kalimat"]
)
singkatan_dict = dict(zip(df_singkatan["singkatan"], df_singkatan["kalimat"]))


def cleansing(text):
    text = re.sub(r"\\", "", text)
    text = re.sub(r"x\d+", " ", text)
    text = text.lower()
    text = re.sub(r"[^A-Za-z0-9]", " ", text)

    return text


def expand_singkatan(text):
    words = text.split()
    expanded_text = []
    for word in words:
        if word in singkatan_dict:
            expanded_text.append(singkatan_dict[word])
        else:
            expanded_text.append(word)
    return " ".join(expanded_text)


pattern_abusive = r"\b(?:" + "|".join(re.escape(word) for word in abusive_list) + r")\b"


def remove_abusive(text):
    text = re.sub(pattern_abusive, "", text)
    return text


conn = sqlite3.connect("data/challenge_gold.db")
cursor = conn.cursor()

cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='uploads_file'"
)
table_exists = cursor.fetchone()

if not table_exists:
    cursor.execute(
        """CREATE TABLE uploads_file (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        csv_data BLOB,
        file_name VARCHAR(255),
        file_type VARCHAR(50)
    )"""
    )

cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='results_file'"
)
table_exists = cursor.fetchone()

if not table_exists:
    cursor.execute(
        """CREATE TABLE results_file (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        csv_data BLOB,
        file_name VARCHAR(255),
        file_type VARCHAR(50)
    )"""
    )

cursor.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='uploads_results_text'"
)
table_exists = cursor.fetchone()

if not table_exists:
    cursor.execute(
        """CREATE TABLE uploads_results_text (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        input_text VARCHAR(255),
        processed_text VARCHAR(255)
    )"""
    )

conn.commit()
conn.close()


DATABASE = "data/challenge_gold.db"


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@swag_from("docs/upload_text.yml", methods=["POST"])
@app.route("/upload-text", methods=["POST"])
def upload_text():
    conn = get_db()
    cursor = conn.cursor()
    input_text = request.form.get("text")

    processed_text = cleansing(processed_text)
    processed_text = expand_singkatan(processed_text)
    processed_text = remove_abusive(processed_text)

    cursor.execute(
        "INSERT INTO uploads_results_text (input_text, processed_text) VALUES (?, ?)",
        (input_text, processed_text),
    )
    conn.commit()
    conn.close()

    json_response = {
        "status_code": 200,
        "description": "Teks yang sudah diproses",
        "data": processed_text,
    }
    response_data = jsonify(json_response)
    return response_data


@swag_from("docs/upload_csv.yml", methods=["POST"])
@app.route("/upload-file", methods=["POST"])
def upload_file():
    conn = get_db()
    cursor = conn.cursor()
    file = request.files["file"]
    if file and allowed_file(file.filename):
        file_content = file.stream.read()

        timestamp = datetime.datetime.now().strftime("%d-%m-%y")
        valid_filename = f"pre_cleansed_data_{timestamp}.csv"

        save_location_pre = os.path.join(UPLOAD_CSV_FOLDER_PRE, valid_filename)

        with open(save_location_pre, "wb") as f:
            f.write(file_content)

        cursor.execute(
            "INSERT INTO uploads_file (csv_data, file_name, file_type) VALUES (?, ?, ?)",
            (file_content, "data.csv", "csv"),
        )

        conn.commit()

        df_pre_censored = pd.read_csv(save_location_pre, encoding="latin1")

        df_pre_censored = df_pre_censored.drop_duplicates()

        df_pre_censored["Tweet"] = df_pre_censored["Tweet"].apply(cleansing)
        df_pre_censored["Tweet"] = df_pre_censored["Tweet"].apply(expand_singkatan)
        df_pre_censored["Tweet"] = df_pre_censored["Tweet"].apply(remove_abusive)

        save_location_post = os.path.join(
            UPLOAD_CSV_FOLDER_POST, f"post_censored_data_{timestamp}.csv"
        )
        df_pre_censored.to_csv(save_location_post, index=False)

        censored_data = df_pre_censored.to_csv(index=False)
        cursor.execute(
            "INSERT INTO results_file (csv_data, file_name, file_type) VALUES (?, ?, ?)",
            (censored_data.encode(), f"post_censored_data_{timestamp}.csv", "csv"),
        )

        conn.commit()
        conn.close()

        json_response = {
            "status_code": 200,
            "description": "File uploaded and processed successfully",
            "data": censored_data,
        }
        response_data = jsonify(json_response)
        return response_data


if __name__ == "__main__":
    app.run(debug=True)
