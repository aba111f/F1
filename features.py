import pandas as pd
RAW_PATH = "f1_all_clean_data.csv"
TRAIN_PATH = "Тима ты сам путь пропиши к себе"

df = pd.read_csv(RAW_PATH)

# ===========================
# 2. REMOVE DNF
# ===========================
df = df[df["Status"].isin(["Finished", "+1 Lap"])]

# ===========================
# 3. POINTS PER RACE
# ===========================
points_map = {1:25, 2:18, 3:15, 4:12, 5:10, 6:8, 7:6, 8:4, 9:2, 10:1}
df["RacePoints"] = df["ClassifiedPosition"].map(points_map).fillna(0)

# ===========================
# 4. CUMULATIVE POINTS
# ===========================
df = df.sort_values(["Year", "Round"])
df["PointsUntilRace"] = df.groupby(["Year", "Abbreviation"])["RacePoints"].cumsum() - df["RacePoints"]

# ===========================
# 5. EXPERIENCE
# ===========================
first_year = df.groupby("Abbreviation")["Year"].min()
df["Experience"] = df.apply(lambda r: r["Year"] - first_year[r["Abbreviation"]], axis=1)

# ===========================
# 6. TRACK TYPE
# ===========================
street = ["Monaco", "Baku", "Jeddah", "Singapore", "Las Vegas", "Miami"]
semi = ["Australia", "Canada", "Saudi Arabia", "Qatar"]

def get_track_type(track):
    if track in street:
        return "street"
    if track in semi:
        return "semi"
    return "permanent"

df["TrackType"] = df["Circuit"].apply(get_track_type)

# ===========================
# 7. FAVORITE TRACK 
# ===========================
avg_finish = df.groupby(["Abbreviation", "Circuit"])["ClassifiedPosition"].mean().reset_index()
avg_finish = avg_finish.rename(columns={"ClassifiedPosition": "AvgFinish"})
df = df.merge(avg_finish, on=["Abbreviation", "Circuit"], how="left")

# ===========================
# 8. GENERATE DRIVER PAIRS
# ===========================
pairs = []

for (year, rnd, team), group in df.groupby(["Year", "Round", "TeamName"]):
    if len(group) != 2:
        continue

    d1, d2 = group.iloc[0], group.iloc[1]

    def make_pair(a, b):
        return {
            "Year": year,
            "Round": rnd,
            "Circuit": a["Circuit"],
            "TeamName": team,
            "DriverA": a["Abbreviation"],
            "DriverB": b["Abbreviation"],

            "Quali_Delta": a["QualiPos"] - b["QualiPos"],
            "Grid_Delta": a["GridPosition"] - b["GridPosition"],
            "Points_Delta": a["PointsUntilRace"] - b["PointsUntilRace"],
            "Experience_Delta": a["Experience"] - b["Experience"],
            "FavoriteTrackAdvantage": a["AvgFinish"] - b["AvgFinish"],
            "TrackType": a["TrackType"],

            "Target": 1 if a["ClassifiedPosition"] < b["ClassifiedPosition"] else 0
        }

    pairs.append(make_pair(d1, d2))

pairs_df = pd.DataFrame(pairs)
pairs_df.to_csv(TRAIN_PATH, index=False)


