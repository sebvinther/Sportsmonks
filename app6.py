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

# Load neural network predictions
@st.cache_data
def load_neural_predictions():
    try:
        return pd.read_csv("upcoming_neural_predictions.csv")
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

# Neural predictions with team names
def get_neural_predictions_with_teams():
    df_neural = load_neural_predictions()
    if df_neural.empty or "fixture_id" not in df_neural.columns:
        return pd.DataFrame()
    
    with get_connection() as conn:
        # Get team names and league info
        fixtures = pd.read_sql("""
            SELECT f.id as fixture_id, f.league_id, f.home_team_id, f.away_team_id, f.starting_at,
                   h.name as home_team_name, a.name as away_team_name, l.name as league_name
            FROM fixtures f
            JOIN teams h ON f.home_team_id = h.id
            JOIN teams a ON f.away_team_id = a.id
            LEFT JOIN leagues l ON f.league_id = l.id
        """, conn)
        
    merged = df_neural.merge(fixtures, on="fixture_id", how="left")
    merged["match"] = merged["home_team_name"] + " vs " + merged["away_team_name"]
    return merged

# Icon mapping
def format_prediction(pred, task):
    if task == "1x2":
        return {0: "🏠", 1: "🤝", 2: "📦"}.get(pred, "❓")
    else:
        return {0: "⬇️", 1: "⬆️"}.get(pred, "❓")

# Enhanced prediction display for neural network
def format_neural_prediction(row):
    """Format neural network predictions with confidence and details"""
    result_map = {0: "🏠 Home Win", 1: "🤝 Draw", 2: "📦 Away Win"}
    ou_map = {0: "⬇️ Under", 1: "⬆️ Over"}
    
    formatted = {
        "Match": row["match"],
        "Date": pd.to_datetime(row["starting_at"]).strftime("%Y-%m-%d %H:%M") if pd.notna(row["starting_at"]) else "TBD",
        "League": row.get("league_name", "Unknown"),
        "1x2 Prediction": result_map.get(row.get("pred_1x2", 0), "❓ Unknown"),
        "O/U 2.5 Goals": ou_map.get(row.get("pred_ou_2_5_goals", 0), "❓ Unknown"),
        "O/U 3.5 Goals": ou_map.get(row.get("pred_ou_3_5_goals", 0), "❓ Unknown")
    }
    return formatted

# === STREAMLIT APP ===
st.title("⚽ Football Intelligence Dashboard")

tabs = st.tabs(["🏆 League Standings", "📊 Stats", "🔮 Classic Predictions", "🧠 Neural Network Predictions"])

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

# Tab 3: Classic Predictions
with tabs[2]:
    st.header("Classic Predictions for Upcoming Fixtures")
    pred_df = get_predictions_with_teams()
    
    if pred_df.empty:
        st.warning("No classic predictions available.")
    else:
        leagues = pred_df["league_id"].dropna().unique()
        selected = st.selectbox("Select League ID", leagues, key="classic_league")
        league_df = pred_df[pred_df["league_id"] == selected]

        tasks = ["1x2", "ou_2_5_goals", "ou_3_5_goals", "ou_2_5_cards", "ou_3_5_cards"]
        show = st.multiselect("Prediction Types", tasks, default=tasks)

        display = league_df[["match", "starting_at"]].copy()
        for task in show:
            for col in league_df.columns:
                if col.startswith(f"pred_{task}"):
                    display[col] = league_df[col].apply(lambda x: format_prediction(x, task))

        st.dataframe(display.sort_values("starting_at"))

# Tab 4: Neural Network Predictions
with tabs[3]:
    st.header("🧠 Neural Network Predictions")
    
    # Add some info about the neural network
    with st.expander("ℹ️ About Neural Network Model"):
        st.markdown("""
        **Model Information:**
        - **Training Data**: 176 matches with complete statistics
        - **Features Used**: Ball Possession %, Corners, Goals, Yellow Cards, Successful Dribbles %
        - **1x2 Accuracy**: 61.1% (Home/Draw/Away predictions)
        - **O/U 2.5 Goals Accuracy**: 66.7% (Over/Under total goals)
        - **Model Type**: Deep Neural Network with dropout regularization
        
        **Prediction Legend:**
        - 🏠 = Home Win
        - 🤝 = Draw  
        - 📦 = Away Win
        - ⬆️ = Over (goals/cards)
        - ⬇️ = Under (goals/cards)
        """)
    
    neural_df = get_neural_predictions_with_teams()
    
    if neural_df.empty:
        st.warning("No neural network predictions available. Please run the neural network training first.")
        st.info("💡 To generate neural predictions, run the `000_neural_sportmonks.ipynb` notebook.")
    else:
        st.success(f"✅ Loaded {len(neural_df)} neural network predictions")
        
        # Filter options
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # League filter
            if "league_name" in neural_df.columns:
                leagues = ["All Leagues"] + sorted(neural_df["league_name"].dropna().unique().tolist())
                selected_league = st.selectbox("Select League", leagues, key="neural_league")
                
                if selected_league != "All Leagues":
                    neural_df = neural_df[neural_df["league_name"] == selected_league]
        
        with col2:
            # Date filter
            if "starting_at" in neural_df.columns and not neural_df["starting_at"].empty:
                neural_df["starting_at"] = pd.to_datetime(neural_df["starting_at"])
                date_range = st.selectbox(
                    "Time Range",
                    ["All Dates", "Next 7 Days", "Next 24 Hours"],
                    key="neural_date"
                )
                
                if date_range == "Next 7 Days":
                    cutoff = pd.Timestamp.now() + pd.Timedelta(days=7)
                    neural_df = neural_df[neural_df["starting_at"] <= cutoff]
                elif date_range == "Next 24 Hours":
                    cutoff = pd.Timestamp.now() + pd.Timedelta(hours=24)
                    neural_df = neural_df[neural_df["starting_at"] <= cutoff]
        
        if len(neural_df) == 0:
            st.warning("No matches found for the selected filters.")
        else:
            # Display options
            view_mode = st.radio(
                "Display Mode",
                ["Detailed View", "Compact View"],
                horizontal=True
            )
            
            if view_mode == "Detailed View":
                # Detailed view with formatted predictions
                st.subheader(f"📋 {len(neural_df)} Upcoming Predictions")
                
                for idx, row in neural_df.head(20).iterrows():  # Show first 20
                    with st.container():
                        col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                        
                        with col1:
                            st.markdown(f"**{row['match']}**")
                            if pd.notna(row.get('starting_at')):
                                date_str = pd.to_datetime(row['starting_at']).strftime("%m/%d %H:%M")
                                st.caption(f"📅 {date_str}")
                            if pd.notna(row.get('league_name')):
                                st.caption(f"🏆 {row['league_name']}")
                        
                        with col2:
                            result_pred = row.get('pred_1x2', 0)
                            result_text = {0: "🏠 Home", 1: "🤝 Draw", 2: "📦 Away"}.get(result_pred, "❓")
                            st.markdown(f"**Result:** {result_text}")
                        
                        with col3:
                            ou25_pred = row.get('pred_ou_2_5_goals', 0)
                            ou25_text = {0: "⬇️ Under 2.5", 1: "⬆️ Over 2.5"}.get(ou25_pred, "❓")
                            st.markdown(f"**Goals:** {ou25_text}")
                        
                        with col4:
                            ou35_pred = row.get('pred_ou_3_5_goals', 0)
                            ou35_text = {0: "⬇️ Under 3.5", 1: "⬆️ Over 3.5"}.get(ou35_pred, "❓")
                            st.markdown(f"**High Score:** {ou35_text}")
                        
                        st.divider()
                
                if len(neural_df) > 20:
                    st.info(f"Showing first 20 of {len(neural_df)} predictions. Use filters to narrow down results.")
                    
            else:
                # Compact table view
                display_df = neural_df.copy()
                
                # Format predictions for display
                if 'pred_1x2' in display_df.columns:
                    display_df['Result'] = display_df['pred_1x2'].map({0: "🏠", 1: "🤝", 2: "📦"})
                if 'pred_ou_2_5_goals' in display_df.columns:
                    display_df['O/U 2.5'] = display_df['pred_ou_2_5_goals'].map({0: "⬇️", 1: "⬆️"})
                if 'pred_ou_3_5_goals' in display_df.columns:
                    display_df['O/U 3.5'] = display_df['pred_ou_3_5_goals'].map({0: "⬇️", 1: "⬆️"})
                
                # Format date
                if 'starting_at' in display_df.columns:
                    display_df['Date'] = pd.to_datetime(display_df['starting_at']).dt.strftime("%m/%d %H:%M")
                
                # Select columns to show
                show_columns = ['match', 'Date', 'league_name', 'Result', 'O/U 2.5', 'O/U 3.5']
                available_columns = [col for col in show_columns if col in display_df.columns]
                
                # Rename columns for display
                column_names = {
                    'match': 'Match',
                    'league_name': 'League',
                    'Date': 'Date & Time'
                }
                display_df = display_df[available_columns].rename(columns=column_names)
                
                st.dataframe(
                    display_df.sort_values('Date & Time' if 'Date & Time' in display_df.columns else display_df.columns[0]),
                    use_container_width=True,
                    hide_index=True
                )
        
        # Summary statistics
        if not neural_df.empty:
            st.subheader("📊 Prediction Summary")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if 'pred_1x2' in neural_df.columns:
                    result_counts = neural_df['pred_1x2'].value_counts()
                    st.metric("🏠 Home Wins", result_counts.get(0, 0))
                    st.metric("🤝 Draws", result_counts.get(1, 0))
                    st.metric("📦 Away Wins", result_counts.get(2, 0))
            
            with col2:
                if 'pred_ou_2_5_goals' in neural_df.columns:
                    ou25_counts = neural_df['pred_ou_2_5_goals'].value_counts()
                    st.metric("⬆️ Over 2.5 Goals", ou25_counts.get(1, 0))
                    st.metric("⬇️ Under 2.5 Goals", ou25_counts.get(0, 0))
            
                if 'pred_ou_3_5_goals' in neural_df.columns:
                    ou35_counts = neural_df['pred_ou_3_5_goals'].value_counts()
                    st.metric("⬆️ Over 3.5 Goals", ou35_counts.get(1, 0))
                    st.metric("⬇️ Under 3.5 Goals", ou35_counts.get(0, 0))