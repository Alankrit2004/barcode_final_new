import os
import threading
import psycopg2
from psycopg2 import pool
import barcode
from barcode.writer import ImageWriter
from flask import Flask, request, jsonify
from supabase import create_client
from dotenv import load_dotenv
import qrcode
from datetime import datetime

load_dotenv()  # Load environment variables from .env file

app = Flask(__name__)

# Supabase Database Connection Pool
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "sslmode": "require"
}

db_pool = pool.SimpleConnectionPool(1, 10, **DB_CONFIG)

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")
QR_SUPABASE_BUCKET = os.getenv("QR_CODE_BUCKET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_db_connection():
    return db_pool.getconn()

def release_db_connection(conn):
    db_pool.putconn(conn)

def generate_unique_id():
    return datetime.now().strftime("%Y%m%d%H%M%S%f")

def generate_barcode(name, quantity):
    """Generates barcodes and uploads to Supabase."""
    unique_id = generate_unique_id()
    barcode_paths = []
    for i in range(quantity):
        barcode_data = f"{name}-{unique_id}-{i}"
        barcode_path = f"/tmp/{barcode_data}.png"
        ean = barcode.get_barcode_class('ean13')
        barcode_instance = ean(barcode_data, writer=ImageWriter())
        barcode_instance.save(barcode_path)
        
        barcode_paths.append(barcode_path)
    return barcode_paths, unique_id

def generate_qr_code(name, quantity):
    """Generates QR codes and uploads to Supabase."""
    unique_id = generate_unique_id()
    qr_paths = []
    for i in range(quantity):
        qr_data = f"{name}-{unique_id}-{i}"
        qr_path = f"/tmp/{qr_data}.png"
        qr = qrcode.make(qr_data)
        qr.save(qr_path)
        
        qr_paths.append(qr_path)
    return qr_paths, unique_id

def upload_to_supabase(image_paths, bucket, folder):
    """Uploads images to Supabase bucket."""
    uploaded_urls = []
    for path in image_paths:
        filename = os.path.basename(path)
        with open(path, "rb") as f:
            supabase.storage.from_(bucket).upload(f"{folder}/{filename}", f, {"content-type": "image/png"})
        uploaded_urls.append(f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{folder}/{filename}")
    return uploaded_urls

def store_codes_in_db(name, unique_id, urls, table):
    """Stores barcode/QR code details in the database."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for url in urls:
            cur.execute(f"INSERT INTO {table} (name, unique_id, image_path) VALUES (%s, %s, %s)", (name, unique_id, url))
        conn.commit()
        cur.close()
        release_db_connection(conn)
        return True
    except Exception as e:
        print(f"Database Error: {e}")
        return False

@app.route('/generate_barcode', methods=['POST'])
def generate_barcode_api():
    """API endpoint to generate barcodes."""
    data = request.json
    name = data.get("name")
    quantity = data.get("quantity", 1)

    if not name or not isinstance(quantity, int) or quantity < 1:
        return jsonify({"isSuccess": False, "message": "Invalid input"}), 400

    barcode_paths, unique_id = generate_barcode(name, quantity)
    uploaded_urls = upload_to_supabase(barcode_paths, SUPABASE_BUCKET, "barcodes")
    if not store_codes_in_db(name, unique_id, uploaded_urls, "barcodes"):
        return jsonify({"isSuccess": False, "message": "Database error"}), 500

    return jsonify({"isSuccess": True, "message": "Barcodes generated", "urls": uploaded_urls}), 201

@app.route('/generate_qrcode', methods=['POST'])
def generate_qrcode_api():
    """API endpoint to generate QR codes."""
    data = request.json
    name = data.get("name")
    quantity = data.get("quantity", 1)

    if not name or not isinstance(quantity, int) or quantity < 1:
        return jsonify({"isSuccess": False, "message": "Invalid input"}), 400

    qr_paths, unique_id = generate_qr_code(name, quantity)
    uploaded_urls = upload_to_supabase(qr_paths, QR_SUPABASE_BUCKET, "qrcodes")
    if not store_codes_in_db(name, unique_id, uploaded_urls, "qr_codes"):
        return jsonify({"isSuccess": False, "message": "Database error"}), 500

    return jsonify({"isSuccess": True, "message": "QR Codes generated", "urls": uploaded_urls}), 201

if __name__ == '__main__':
    app.run(port=5001, threaded=True)
