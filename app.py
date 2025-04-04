from flask import Flask, request, jsonify
from pymongo import MongoClient
import asyncio
from datetime import datetime
import requests
import os

app = Flask(__name__)

# MongoDB connection with pymongo (synchronous)
client = MongoClient('mongodb+srv://shekharhatture107:593l9WPPjJ9y5HXm@cluster0.frrrs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client['movies_db']
movies_collection = db['movies']

# Telegram API setup
BOT_TOKEN = "8181263340:AAFoljjOFqe7b24u708_mXt3zkq2El1n70Y"
CHANNEL_ID = "-1002605592823"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Movie data model (synchronous with pymongo)
def save_movie(data):
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
    
    movie = {k: v for k, v in movie.items() if v is not None}
    result = movies_collection.insert_one(movie)  # Synchronous call
    return movie, str(result.inserted_id)

def format_direct_links(links_data):
    formatted_links = {}
    for platform, qualities in links_data.items():
        formatted_links[platform] = {}
        if isinstance(qualities, dict):
            for quality, link in qualities.items():
                formatted_links[platform][quality] = link
    return formatted_links

async def send_to_telegram(movie):
    caption = (
        f"🎬 <b>{movie.get('title', 'N/A')}</b>\n"
        f"📅 <b>Released:</b> {movie.get('released', 'N/A')}\n"
        f"🎭 <b>Genre:</b> {movie.get('genre', 'N/A')}\n"
        f"📝 <b>Plot:</b> {movie.get('plot', 'N/A')}\n"
        f"⏱️ <b>Runtime:</b> {movie.get('runtime', 'N/A')}\n"
        f"⭐ <b>IMDb:</b> {movie.get('imdb', 'N/A')}\n\n"
        "<b>Screenshots:</b>\n"
    )
    
    screenshots = movie.get('screenshots', [])
    if screenshots:
        caption += "\n".join([f"📸 {url}" for url in screenshots]) + "\n\n"
    else:
        caption += "No screenshots available\n\n"

    caption += "<b>Direct Links:</b>\n"
    direct_links = movie.get('direct_links', {})
    for platform, qualities in direct_links.items():
        caption += f"📦 <b>{platform}:</b>\n"
        for quality, link in qualities.items():
            caption += f"  {quality}: <a href='{link}'>{link}</a>\n"
        caption += "\n"

    try:
        poster_url = movie.get('poster')
        if poster_url:
            payload = {
                "chat_id": CHANNEL_ID,
                "photo": poster_url,
                "caption": caption,
                "parse_mode": "HTML"
            }
            response = await asyncio.to_thread(requests.post, f"{TELEGRAM_API_URL}/sendPhoto", json=payload)
        else:
            payload = {
                "chat_id": CHANNEL_ID,
                "text": caption,
                "parse_mode": "HTML"
            }
            response = await asyncio.to_thread(requests.post, f"{TELEGRAM_API_URL}/sendMessage", json=payload)

        if response.status_code != 200:
            print(f"Failed to send to Telegram: {response.text}")
        else:
            print("Successfully sent to Telegram")
    except Exception as e:
        print(f"Error sending to Telegram: {str(e)}")

@app.route('/api/movies', methods=['POST'])
async def create_movie():
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
        
        movie, movie_id = save_movie(movie_data)  # Synchronous call
        await send_to_telegram(movie)  # Async call for Telegram
        
        return jsonify({"status": "success", "movie_id": movie_id}), 201
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/movies', methods=['GET'])
def get_all_movies():
    try:
        # Fetch all movies from the collection (synchronous with pymongo)
        movies = list(movies_collection.find({}))
        if not movies:
            return jsonify({"status": "success", "movies": [], "message": "No movies found"}), 200
        
        # Convert ObjectId to string for JSON serialization
        for movie in movies:
            movie['_id'] = str(movie['_id'])
            # Convert datetime objects to strings (optional, for readability)
            movie['created_at'] = movie['created_at'].isoformat()
            movie['updated_at'] = movie['updated_at'].isoformat()
        
        return jsonify({"status": "success", "movies": movies}), 200
    
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
