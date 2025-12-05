import requests

BASE_URL = "https://api.balldontlie.io/v1"

def get_games_by_date(date, API_KEY):
    url = f"{BASE_URL}/games"
    headers = {
        "Authorization": API_KEY
    }
    params = {
        "dates[]": date,
    }
    r = requests.get(url, headers=headers, params=params)
    print("Status:", r.status_code)
    if r.status_code != 200:
        return r.status_code
    data = r.json()
    games = data.get("data", [])
    results = []
    for g in games:
        results.append({
            "game_id": g["id"],
            "date": g["date"],
            "home_team": g["home_team"]["full_name"],
            "away_team": g["visitor_team"]["full_name"]
        })
    return results