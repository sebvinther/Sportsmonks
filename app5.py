import streamlit as st
import pandas as pd
import sqlite3

st.set_page_config(page_title="Football Dashboard", layout="wide")

# Connect to DB
def get_connection():
    return sqlite3.connect("db_sportmonks.db")

# Load all predictions from CSV (fallback)
@st.cache_data
def load_predictions():
    try:
        return pd.read_csv("upcoming_predictions.csv")
    except:
        return pd.DataFrame()

# League selector from DB
def get_leagues_from_standings():
    with get_connection() as conn:
        df = pd.read_sql("SELECT DISTINCT league_id FROM standings", conn)
    return df["league_id"].tolist()

# Fixtures with predictions, merged with teams
def get_predictions_with_teams():
    df_preds = load_predictions()
    if df_preds.empty or "fixture_id" not in df_preds.columns:
        return pd.DataFrame()
    
    with get_connection() as conn:
        # Get team names
        fixtures = pd.read_sql("""
            SELECT f.id as fixture_id, f.league_id, f.home_team_id, f.away_team_id, f.starting_at,
                   h.name as home_team_name, a.name as away_team_name
            FROM fixtures f
            JOIN teams h ON f.home_team_id = h.id
            JOIN teams a ON f.away_team_id = a.id
        """, conn)
        
    merged = df_preds.merge(fixtures, on="fixture_id", how="left")
    merged["match"] = merged["home_team_name"] + " vs " + merged["away_team_name"]
    return merged

# Icon mapping
def format_prediction(pred, task):
    if task == "1x2":
        return {0: "🏠", 1: "🤝", 2: "📦"}.get(pred, "❓")
    else:
        return {0: "⬇️", 1: "⬆️"}.get(pred, "❓")

# === STREAMLIT APP ===
st.title("⚽ Football Intelligence Dashboard")

tabs = st.tabs(["🏆 League Standings", "📊 Stats", "🔮 Predictions"])

# Tab 1: Standings
with tabs[0]:
    st.header("League Standings")
    leagues = get_leagues_from_standings()
    selected_league = st.selectbox("Select League ID", leagues)
    
    with get_connection() as conn:
        df = pd.read_sql(f"SELECT * FROM standings WHERE league_id = {selected_league}", conn)
    st.dataframe(df)

# Tab 2: Stats (Placeholder)
with tabs[1]:
    st.header("Team / Player Statistics")
    st.markdown("More stats coming soon...")

# Tab 3: Predictions
with tabs[2]:
    st.header("Predictions for Upcoming Fixtures")
    pred_df = get_predictions_with_teams()
    
    if pred_df.empty:
        st.warning("No predictions available.")
    else:
        leagues = pred_df["league_id"].dropna().unique()
        selected = st.selectbox("Select League ID", leagues)
        league_df = pred_df[pred_df["league_id"] == selected]

        tasks = ["1x2", "ou_2_5_goals", "ou_3_5_goals", "ou_2_5_cards", "ou_3_5_cards"]
        show = st.multiselect("Prediction Types", tasks, default=tasks)

        display = league_df[["match", "starting_at"]].copy()
        for task in show:
            for col in league_df.columns:
                if col.startswith(f"pred_{task}"):
                    display[col] = league_df[col].apply(lambda x: format_prediction(x, task))

        st.dataframe(display.sort_values("starting_at"))
