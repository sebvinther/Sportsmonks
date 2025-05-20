import streamlit as st
import pandas as pd
import pickle
import os
import seaborn as sns

import matplotlib.pyplot as plt

st.set_page_config(page_title="Football Predictions", layout="wide")

# --- Load predictions ---
@st.cache_data
def load_predictions(file_path='all_leagues_predictions.pkl'):
    if not os.path.exists(file_path):
        return None
    with open(file_path, 'rb') as f:
        preds = pickle.load(f)
    return pd.DataFrame(preds) if isinstance(preds, list) else preds

predictions_file = st.sidebar.text_input("Predictions file path", "all_leagues_predictions.pkl")
df = load_predictions(predictions_file)

if df is None or df.empty:
    st.warning("No predictions found. Please check the file path.")
    st.stop()

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Predictions Table", "Recommended Bets", "Statistics"])

# --- Tab 1: Predictions Table ---
with tab1:
    st.header("All Match Predictions")
    leagues = ["All"] + sorted(df['league_name'].unique())
    league = st.selectbox("Filter by league", leagues)
    filtered = df if league == "All" else df[df['league_name'] == league]
    st.dataframe(filtered[['league_name', 'match_date', 'home_team', 'away_team', 'predictions']], use_container_width=True)

# --- Tab 2: Recommended Bets ---
with tab2:
    st.header("Recommended Bets (High Confidence)")
    recs = []
    for _, row in df.iterrows():
        preds = row['predictions']
        match = f"{row['home_team']} vs {row['away_team']}"
        # Result
        if 'result' in preds and isinstance(preds['result'], dict):
            conf = preds['result'].get('confidence', 0)
            if conf and conf >= 0.6:
                recs.append({
                    "Match": match,
                    "Type": "Result",
                    "Pick": preds['result']['prediction'],
                    "Confidence": f"{conf:.2f}"
                })
        # Over/Under
        if 'over_under' in preds and isinstance(preds['over_under'], dict):
            conf = preds['over_under'].get('confidence', 0)
            if conf and conf >= 0.55:
                recs.append({
                    "Match": match,
                    "Type": "Over/Under 2.5",
                    "Pick": preds['over_under']['prediction'],
                    "Confidence": f"{conf:.2f}"
                })
        # Cards
        if 'cards' in preds and isinstance(preds['cards'], dict):
            conf = preds['cards'].get('confidence', 0)
            if conf and conf >= 0.6:
                recs.append({
                    "Match": match,
                    "Type": "Cards O/U",
                    "Pick": preds['cards']['prediction'],
                    "Confidence": f"{conf:.2f}"
                })
    if recs:
        st.dataframe(pd.DataFrame(recs))
    else:
        st.info("No high-confidence bets found.")

# --- Tab 3: Statistics ---
with tab3:
    st.header("Prediction Statistics")
    # Pie chart for result predictions
    result_counts = {"Home Win": 0, "Draw": 0, "Away Win": 0}
    for _, row in df.iterrows():
        preds = row['predictions']
        if 'result' in preds and isinstance(preds['result'], dict):
            pred = preds['result'].get('prediction')
            if pred in result_counts:
                result_counts[pred] += 1
    st.subheader("Match Result Prediction Distribution")
    fig1, ax1 = plt.subplots()
    ax1.pie(result_counts.values(), labels=result_counts.keys(), autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')
    st.pyplot(fig1)

    # Over/Under distribution
    ou_counts = {"Over": 0, "Under": 0}
    for _, row in df.iterrows():
        preds = row['predictions']
        if 'over_under' in preds and isinstance(preds['over_under'], dict):
            pred = preds['over_under'].get('prediction')
            if pred in ou_counts:
                ou_counts[pred] += 1
    st.subheader("Over/Under 2.5 Prediction Distribution")
    fig2, ax2 = plt.subplots()
    ax2.pie(ou_counts.values(), labels=ou_counts.keys(), autopct='%1.1f%%', startangle=90)
    ax2.axis('equal')
    st.pyplot(fig2)

    # Confidence KDE
    st.subheader("Prediction Confidence Distribution")
    confs = []
    for _, row in df.iterrows():
        preds = row['predictions']
        for key in ['result', 'over_under', 'cards']:
            if key in preds and isinstance(preds[key], dict):
                c = preds[key].get('confidence')
                if c is not None:
                    confs.append(c)
    if confs:
        fig3, ax3 = plt.subplots()
        sns.kdeplot(confs, ax=ax3)
        ax3.set_xlabel("Confidence")
        ax3.set_title("All Prediction Confidences")
        st.pyplot(fig3)
    else:
        st.info("No confidence data available.")


