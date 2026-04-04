import requests
from datetime import datetime
import json

def get_season_baja_bombs():
    # Use the ISO format which the Search API prefers
    start_date = "2026-03-20" 
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    # We add gameType=R for Regular Season and explicitly ask for hit_distance_sc
    url = (
        f"https://statsapi.mlb.com/api/v1/statcast/search?sportId=1"
        f"&gameType=R&startDate={start_date}&endDate={end_date}"
        f"&hitResult=Home+Run&limit=1000"
    )
    
    print(f"Checking for bombs since {start_date}...")

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('data', [])
        
        baja_list = []
        for play in data:
            # The distance can sometimes be in different fields depending on the API version
            dist = play.get('hit_distance_sc') or play.get('launch_data', {}).get('distance', 0)
            
            if dist and float(dist) >= 420:
                baja_list.append({
                    "player": play.get('player_name', 'Unknown Slugger'),
                    "distance": int(dist),
                    "team": play.get('team_name', 'MLB'),
                    "opponent": play.get('opponent_name', 'Opponent'),
                    "date": play.get('game_date', '2026-00-00')
                })
        
        # Remove duplicates (sometimes search returns multiple entries for one play)
        unique_list = { (h['player'], h['date']): h for h in baja_list }.values()
        sorted_list = sorted(unique_list, key=lambda x: x['distance'], reverse=True)
        
        with open('data.json', 'w') as f:
            json.dump(list(sorted_list), f, indent=4)
            
        print(f"Success! Found {len(sorted_list)} Baja Blasts.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    get_season_baja_bombs()