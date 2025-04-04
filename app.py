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
    
    # Skip if direct_links is empty or None
    if not movie.get("direct_links") or movie["direct_links"] == {}:
        print(f"Skipping '{movie['title']}' due to empty or None direct_links")
        return None, None
    
    # Check if a movie/series with the same title already exists
    existing_movie = movies_collection.find_one({"title": movie["title"]})
    
    if existing_movie:
        # Compare existing movie with new data (excluding timestamps and _id)
        existing_data = {k: v for k, v in existing_movie.items() if k not in ['_id', 'created_at', 'updated_at']}
        new_data = {k: v for k, v in movie.items() if k not in ['created_at', 'updated_at']}
        
        if existing_data != new_data:
            # Update the existing movie if data has changed
            movie["created_at"] = existing_movie["created_at"]  # Preserve original creation time
            movies_collection.update_one({"title": movie["title"]}, {"$set": movie})
            print(f"Updated movie/series: {movie['title']}")
            return movie, str(existing_movie["_id"])
        else:
            print(f"Movie/series already exists with identical data: {movie['title']}")
            return existing_movie, str(existing_movie["_id"])
    else:
        # Insert new movie/series if it doesn’t exist
        result = movies_collection.insert_one(movie)
        print(f"Inserted new movie/series: {movie['title']}")
        return movie, str(result.inserted_id)

def format_direct_links(links_data):
    formatted_links = {}
    for key, value in links_data.items():
        if "note" in key.lower():
            continue  # Skip platforms with "note" in the name
        
        if key.lower() in ["zip batch", "single episodes"] or "epi" in key.lower():
            # Handle episodic or batch data
            if isinstance(value, dict):
                if key.lower() == "zip batch":
                    formatted_links[key] = {}
                    for quality, providers in value.items():
                        if isinstance(providers, dict):
                            formatted_links[key][quality] = providers
                elif key.lower() == "single episodes" or "epi" in key.lower():
                    formatted_links[key] = {}
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, dict):
                            formatted_links[key][sub_key] = sub_value
        else:
            # Handle non-episodic data
            formatted_links[key] = {}
            if isinstance(value, dict):
                for quality, link in value.items():
                    formatted_links[key][quality] = link
    
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
        if movie is None and movie_id is None:
            return jsonify({"status": "skipped", "message": "Movie skipped due to empty direct_links"}), 200
        
        return jsonify({"status": "success", "movie_id": movie_id}), 201
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/movies', methods=['GET'])
def get_all_movies():
    try:
        # Fetch all movies and filter out those with empty or None direct_links
        movies = [movie for movie in movies_collection.find({}) if movie.get("direct_links") and movie["direct_links"] != {}]
        if not movies:
            return jsonify({"status": "success", "movies": [], "message": "No movies found with valid direct_links"}), 200
        
        for movie in movies:
            movie['_id'] = str(movie['_id'])
            movie['created_at'] = movie['created_at'].isoformat()
            movie['updated_at'] = movie['updated_at'].isoformat()
        
        return jsonify({"status": "success", "movies": movies}), 200
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/movies/remove-duplicates', methods=['POST'])
def remove_duplicates():
    try:
        # Get all movies sorted by updated_at (newest first)
        all_movies = list(movies_collection.find({}).sort("updated_at", -1))
        if not all_movies:
            return jsonify({"status": "success", "message": "No movies to process"}), 200
        
        # Track seen titles and duplicates to remove
        seen_titles = set()
        duplicates_removed = 0
        
        for movie in all_movies:
            title = movie["title"]
            if title in seen_titles:
                # Remove duplicate (keep the most recent one due to sort order)
                movies_collection.delete_one({"_id": movie["_id"]})
                duplicates_removed += 1
                print(f"Removed duplicate movie: {title}")
            else:
                seen_titles.add(title)
        
        return jsonify({
            "status": "success",
            "message": f"Removed {duplicates_removed} duplicate movies",
            "remaining_movies": len(all_movies) - duplicates_removed
        }), 200
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)
