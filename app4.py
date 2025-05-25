# app.py
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Upcoming Match Predictions", layout="wide")

st.title("🔮 Upcoming Football Predictions + Odds")

# Load your predictions with odds
df = pd.read_csv("upcoming_predictions.csv")  # You should save `preds_df` to this file after merging odds

# Filters
selected_fixtures = st.multiselect("Select Fixture IDs", options=df["fixture_id"].unique(), default=df["fixture_id"].head(10))

# Display filtered data
st.dataframe(df[df["fixture_id"].isin(selected_fixtures)])
