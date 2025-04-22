from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection
MONGO_URL = "mongodb+srv://shekharhatture107:593l9WPPjJ9y5HXm@cluster0.frrrs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URL)
db = client["Cluster0"]

# MongoDB collection
collection = db["Takecharacters4"]

@app.route('/api/characters', methods=['GET'])
def get_all_characters():
    try:
        # Fetch all documents from the collection
        characters = list(collection.find({}, {"_id": 0}))  # Exclude the MongoDB ObjectID field
        return jsonify({"status": "success", "data": characters}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
