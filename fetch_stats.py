import requests
from datetime import datetime, timedelta
import json

def get_420_homers():
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # MLB Statcast Search URL for HRs >= 420ft
    url = f"https://statsapi.mlb.com/api/v1/statcast/search?sportId=1&startDate={start_date}&endDate={end_date}&hitResult=Home+Run&minDistance=420"
    
    # Note: If the search endpoint is restricted, we fallback to game-by-game processing 
    # For this example, we'll assume a simplified data structure from the API
    response = requests.get(url)
    all_data = response.json().get('data', [])
    
    formatted_hrs = []
    for play in all_data:
        formatted_hrs.append({
            "player": play['player_name'],
            "distance": play['hit_distance_sc'],
            "team": play['team_name'],
            "opponent": play['opponent_name'],
            "date": play['game_date']
        })
    
    # Sort by distance descending
    formatted_hrs.sort(key=lambda x: x['distance'], reverse=True)
    
    with open('data.json', 'w') as f:
        json.dump(formatted_hrs, f, indent=4)

if __name__ == "__main__":
    get_420_homers()