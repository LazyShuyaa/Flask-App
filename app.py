from flask import Flask, send_file, jsonify
from pymongo import MongoClient
import sqlite3
import os
from io import BytesIO

app = Flask(__name__)

# MongoDB connection
MONGO_URL = "mongodb+srv://shekharhatture107:593l9WPPjJ9y5HXm@cluster0.frrrs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URL)
db = client["Cluster0"]

# MongoDB collection
collection = db["Takecharacters4"]

# Convert MongoDB collection to SQLite database
def convert_mongo_to_sqlite():
    # Create an in-memory SQLite database
    sqlite_db = sqlite3.connect(":memory:")
    cursor = sqlite_db.cursor()

    # Create table
    cursor.execute('''
        CREATE TABLE characters (
            character_id TEXT PRIMARY KEY,
            image TEXT,
            name TEXT,
            anime TEXT,
            rarity TEXT,
            uploader_id TEXT,
            uploader_name TEXT,
            event TEXT
        )
    ''')

    # Fetch MongoDB data
    characters = collection.find()

    # Insert data into SQLite
    for character in characters:
        cursor.execute('''
            INSERT OR REPLACE INTO characters (character_id, image, name, anime, rarity, uploader_id, uploader_name, event)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            character.get("character_id"),
            character.get("image"),
            character.get("name"),
            character.get("anime"),
            character.get("rarity"),
            character.get("uploader_id"),
            character.get("uploader_name"),
            character.get("event", "")
        ))

    # Commit changes and return the SQLite database in memory
    sqlite_db.commit()

    return sqlite_db

@app.route('/download_sqlite', methods=['GET'])
def download_sqlite():
    try:
        # Convert MongoDB data to SQLite
        sqlite_db = convert_mongo_to_sqlite()

        # Create a file-like object to send as a response
        with BytesIO() as db_file:
            for chunk in sqlite_db.iterdump():
                db_file.write(chunk.encode("utf-8"))
            db_file.seek(0)

            # Send SQLite file as a response
            return send_file(db_file, as_attachment=True, download_name="characters.db", mimetype="application/x-sqlite3")
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
