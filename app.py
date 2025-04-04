from flask import Flask, request, jsonify
from pymongo import MongoClient
from datetime import datetime
import os

app = Flask(__name__)

# MongoDB connection with pymongo (synchronous)
client = MongoClient('mongodb+srv://shekharhatture107:593l9WPPjJ9y5HXm@cluster0.frrrs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['filmdb']
movies_collection = db['movies']

# Movie data model (synchronous with pymongo)
def save_or_update_movie(data):
    movie = {
        "title": data.get("title"),
        "type": data.get("type", "movie"),
        "released": data.get("released"),
        "poster": data.get("poster"),
        "genre": data.get("genre"),
        "plot": data.get("plot"),
        "runtime": data.get("runtime"),
        "imdb": data.get("imdb"),
        "screenshots": data.get("screenshots", []),
        "direct_links": format_direct_links(data.get("direct_links", {})),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow()
    }
    
    # Remove None values
    movie = {k: v for k, v in movie.items() if v is not None}
    
    # Check if a movie with the same title already exists
    existing_movie = movies_collection.find_one({"title": movie["title"]})
    
    if existing_movie:
        # Compare existing movie with new data (excluding timestamps and _id)
        existing_data = {k: v for k, v in existing_movie.items() if k not in ['_id', 'created_at', 'updated_at']}
        new_data = {k: v for k, v in movie.items() if k not in ['created_at', 'updated_at']}
        
        if existing_data != new_data:
            # Update the existing movie if data has changed
            movie["created_at"] = existing_movie["created_at"]  # Preserve original creation time
            movies_collection.update_one({"title": movie["title"]}, {"$set": movie})
            print(f"Updated movie: {movie['title']}")
            return movie, str(existing_movie["_id"])
        else:
            print(f"Movie already exists with identical data: {movie['title']}")
            return existing_movie, str(existing_movie["_id"])
    else:
        # Insert new movie if it doesn’t exist
        result = movies_collection.insert_one(movie)
        print(f"Inserted new movie: {movie['title']}")
        return movie, str(result.inserted_id)

def format_direct_links(links_data):
    formatted_links = {}
    for platform, qualities in links_data.items():
        # Skip platforms with "note" (case-insensitive)
        if "note" not in platform.lower():
            formatted_links[platform] = {}
            if isinstance(qualities, dict):
                for quality, link in qualities.items():
                    formatted_links[platform][quality] = link
    return formatted_links

@app.route('/api/movies', methods=['POST'])
def create_movie():
    try:
        data = request.get_json()
        
        movie_data = {
            "title": data.get("Title"),
            "type": data.get("Type"),
            "released": data.get("Released"),
            "poster": data.get("Poster"),
            "genre": data.get("Genre"),
            "plot": data.get("Plot"),
            "runtime": data.get("Runtime"),
            "imdb": data.get("IMDb"),
            "screenshots": data.get("Screenshots", []),
            "direct_links": data.get("Direct Links", {})
        }
        
        movie, movie_id = save_or_update_movie(movie_data)
        
        return jsonify({"status": "success", "movie_id": movie_id}), 201
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/movies', methods=['GET'])
def get_all_movies():
    try:
        movies = list(movies_collection.find({}))
        if not movies:
            return jsonify({"status": "success", "movies": [], "message": "No movies found"}), 200
        
        for movie in movies:
            movie['_id'] = str(movie['_id'])
            movie['created_at'] = movie['created_at'].isoformat()
            movie['updated_at'] = movie['updated_at'].isoformat()
        
        return jsonify({"status": "success", "movies": movies}), 200
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)
