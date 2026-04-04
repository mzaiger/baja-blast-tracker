"""
Run this to inspect what the MLB live feed actually returns.
It'll print the raw structure of any home run plays found today.
"""
import requests
import json
from datetime import datetime

today = datetime.now().strftime('%Y-%m-%d')

# Step 1: get today's games
sched = requests.get(
    f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&gameType=R",
    timeout=10
).json()

game_pks = []
for date_block in sched.get("dates", []):
    for game in date_block.get("games", []):
        game_pks.append(game["gamePk"])

print(f"Games today: {game_pks}\n")

for pk in game_pks:  # limit to first 2 games
    feed = requests.get(
        f"https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live",
        timeout=15
    ).json()

    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    print(f"\n=== Game {pk} — {len(plays)} plays ===")

    for play in plays:
        result = play.get("result", {})
        event       = result.get("event", "")
        event_type  = result.get("eventType", "")

        # print ALL unique event types so we can see what home runs look like
        if "home" in event.lower() or "home" in event_type.lower():
            print(f"\n🏠 HIT — event='{event}'  eventType='{event_type}'")
            print(f"   hitData: {json.dumps(play.get('hitData', {}), indent=2)}")
            print(f"   batter:  {play.get('matchup',{}).get('batter',{}).get('fullName')}")

        # also just dump all unique eventTypes seen
    unique_events = set(p.get("result", {}).get("eventType", "") for p in plays)
    print(f"\nAll eventTypes in game {pk}: {unique_events}")
