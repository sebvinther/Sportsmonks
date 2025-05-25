
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="⚽ Football Predictions", page_icon="⚽", layout="wide")

st.title("⚽ AI Football Prediction System")
st.markdown("---")

# Load predictions
try:
    predictions = pd.read_csv('balanced_predictions.csv')
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Predictions", len(predictions))
    
    with col2:
        avg_confidence = predictions['confidence'].mean()
        st.metric("Avg Confidence", f"{avg_confidence:.3f}")
    
    with col3:
        high_conf = len(predictions[predictions['confidence'] > 0.6])
        st.metric("High Confidence", high_conf)
    
    with col4:
        model_acc = predictions['model_accuracy'].iloc[0]
        st.metric("Model Accuracy", model_acc)
    
    # Distribution chart
    st.subheader("📊 Prediction Distribution")
    pred_counts = predictions['pred_1x2'].value_counts()
    labels = ['Home Win', 'Draw', 'Away Win']
    
    fig = px.pie(values=pred_counts.values, names=[labels[i] for i in pred_counts.index])
    st.plotly_chart(fig, use_container_width=True)
    
    # Top predictions
    st.subheader("🏆 Top Predictions")
    top_preds = predictions.nlargest(10, 'confidence')
    
    for _, pred in top_preds.iterrows():
        outcome = labels[pred['pred_1x2']]
        
        with st.expander(f"{pred['home_team']} vs {pred['away_team']} - {outcome}"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**Prediction:** {outcome}")
                st.write(f"**Confidence:** {pred['confidence']:.3f}")
                st.write(f"**Recommendation:** {pred['recommendation']}")
            
            with col2:
                st.write("**Probabilities:**")
                st.write(f"Home: {pred['prob_home']:.3f}")
                st.write(f"Draw: {pred['prob_draw']:.3f}")
                st.write(f"Away: {pred['prob_away']:.3f}")
    
    # All predictions table
    st.subheader("📋 All Predictions")
    display_df = predictions[['home_team', 'away_team', 'pred_1x2', 'confidence', 'recommendation']].copy()
    display_df['pred_1x2'] = display_df['pred_1x2'].map({0: 'Home Win', 1: 'Draw', 2: 'Away Win'})
    st.dataframe(display_df, use_container_width=True)
    
except FileNotFoundError:
    st.error("❌ No predictions found. Please run the prediction system first.")
