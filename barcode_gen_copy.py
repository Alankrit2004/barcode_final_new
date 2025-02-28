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
import time

load_dotenv()  # Load environment variables from .env file

app = Flask(__name__)

# Supabase Database Connection Pool
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "sslmode": "require"  # Enforce SSL connection
}

db_pool = pool.SimpleConnectionPool(1, 10, **DB_CONFIG)

# Supabase Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_db_connection():
    return db_pool.getconn()

def release_db_connection(conn):
    db_pool.putconn(conn)

def generate_unique_id():
    return str(int(time.time()))

def generate_gs1_barcode(unique_id):
    """Generates GS1 barcode and saves it to the /tmp directory."""
    try:
        barcode_path = f"/tmp/{unique_id}"
        ean = barcode.get_barcode_class('ean13')
        barcode_instance = ean(unique_id, writer=ImageWriter())

        full_path = barcode_instance.save(barcode_path)

        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Barcode image not created at {full_path}")

        return full_path
    except Exception as e:
        print(f"Error generating barcode: {e}")
        return None

def generate_qr_code(data, unique_id):
    """Generates a QR code and saves it as an image in the /tmp directory."""
    try:
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(data)
        qr.make(fit=True)

        qr_path = f"/tmp/{unique_id}_qr.png"
        img = qr.make_image(fill="black", back_color="white")
        img.save(qr_path)

        return qr_path
    except Exception as e:
        print(f"Error generating QR Code: {e}")
        return None

def upload_to_supabase(image_path, unique_id, file_type):
    """Uploads barcode or QR code image to Supabase Storage and returns the public URL."""
    try:
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"File not found: {image_path}")

        folder = "barcodes" if file_type == "barcode" else "qrcodes"

        with open(image_path, "rb") as f:
            response = supabase.storage.from_(SUPABASE_BUCKET).upload(
                f"static/{folder}/{unique_id}.png", f, {"content-type": "image/png"}
            )

        public_url = f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}/static/{folder}/{unique_id}.png"
        return public_url
    except Exception as e:
        print(f"Error uploading to Supabase: {e}")
        return None

def store_product_in_db(name, unique_id, image_url, image_type):
    """Stores product details in the database."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        table = "barcodes" if image_type == "barcode" else "qrcodes"
        cur.execute(
            f"INSERT INTO {table} (name, unique_id, image_path) VALUES (%s, %s, %s)",
            (name, unique_id, image_url)
        )
        conn.commit()
        cur.close()
        release_db_connection(conn)
    except Exception as e:
        print(f"Database Error: {e}")
        return False
    return True

@app.route('/generate_barcode_new', methods=['POST'])
def generate_barcode():
    """API endpoint to generate a barcode."""
    data = request.json
    name = data.get("name")
    quantity = data.get("quantity", 1)

    if not name:
        return jsonify({"isSuccess": False, "message": "Missing required fields"}), 400

    def generate_unique_id():
        return str(int(time.time()))[-12:].zfill(12)  # Ensure 12 digits

    unique_id = generate_unique_id()


    barcode_path = generate_gs1_barcode(unique_id)
    if not barcode_path:
        return jsonify({"isSuccess": False, "message": "Failed to generate barcode"}), 500

    barcode_url = upload_to_supabase(barcode_path, unique_id, "barcode")
    if not barcode_url:
        return jsonify({"isSuccess": False, "message": "Failed to upload barcode"}), 500

    if not store_product_in_db(name, unique_id, barcode_url, "barcode"):
        return jsonify({"isSuccess": False, "message": "Database error"}), 500

    return jsonify({
        "isSuccess": True,
        "message": "Barcode generated and stored successfully",
        "unique_id": unique_id,
        "barcode_image_path": barcode_url
    }), 201

@app.route('/generate_qrcode_new', methods=['POST'])
def generate_qrcode():
    """API endpoint to generate a QR code."""
    data = request.json
    name = data.get("name")
    quantity = data.get("quantity", 1)

    if not name:
        return jsonify({"isSuccess": False, "message": "Missing required fields"}), 400

    unique_id = generate_unique_id()

    qr_path = generate_qr_code(name, unique_id)
    if not qr_path:
        return jsonify({"isSuccess": False, "message": "Failed to generate QR Code"}), 500

    qr_url = upload_to_supabase(qr_path, unique_id, "qrcode")
    if not qr_url:
        return jsonify({"isSuccess": False, "message": "Failed to upload QR Code"}), 500

    if not store_product_in_db(name, unique_id, qr_url, "qrcode"):
        return jsonify({"isSuccess": False, "message": "Database error"}), 500

    return jsonify({
        "isSuccess": True,
        "message": "QR Code generated and stored successfully",
        "unique_id": unique_id,
        "qr_code_image_path": qr_url
    }), 201

@app.route('/scan_code_new', methods=['POST'])
def scan_code():
    """API endpoint to scan a barcode or QR code and retrieve product details."""
    data = request.json
    unique_id = data.get("unique_id")

    if not unique_id:
        return jsonify({"isSuccess": False, "message": "Unique ID is required"}), 400

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT name, image_path FROM barcodes WHERE unique_id = %s UNION SELECT name, image_path FROM qrcodes WHERE unique_id = %s", (unique_id, unique_id))
        product = cur.fetchone()
        cur.close()
        release_db_connection(conn)

        if not product:
            return jsonify({"isSuccess": False, "message": "Product not found"}), 404

        return jsonify({"isSuccess": True, "name": product[0], "image_path": product[1]}), 200
    except Exception as e:
        print(f"Database Error: {e}")
        return jsonify({"isSuccess": False, "message": "Database error"}), 500

if __name__ == '__main__':
    app.run(port=5001, threaded=True)
