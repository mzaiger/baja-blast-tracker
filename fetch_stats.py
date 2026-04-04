import requests
from datetime import datetime
import json

def get_season_baja_bombs():
    # Start of 2026 Season/Spring Training
    start_date = "03/01/2026" 
    # Today's date
    end_date = datetime.now().strftime('%m/%d/%Y')
    
    # We add &limit=500 to make sure we don't miss any throughout the year
    url = f"https://statsapi.mlb.com/api/v1/statcast/search?sportId=1&startDate={start_date}&endDate={end_date}&hitResult=Home+Run&limit=500"
    
    try:
        response = requests.get(url)
        data = response.json().get('data', [])
        
        baja_list = []
        for play in data:
            dist = play.get('hit_distance_sc', 0)
            
            # The 420' Club Filter
            if dist and dist >= 420:
                baja_list.append({
                    "player": play.get('player_name', 'Unknown'),
                    "distance": dist,
                    "team": play.get('team_name', 'MLB'),
                    "opponent": play.get('opponent_name', 'Opponent'),
                    "date": play.get('game_date', '2026-00-00')
                })
        
        # Sort by distance (Longest first)
        baja_list.sort(key=lambda x: x['distance'], reverse=True)
        
        with open('data.json', 'w') as f:
            json.dump(baja_list, f, indent=4)
        print(f"Successfully tracked {len(baja_list)} Baja Blasts for the 2026 season.")

    except Exception as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    get_season_baja_bombs()