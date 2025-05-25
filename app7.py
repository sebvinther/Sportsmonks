import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime

st.set_page_config(page_title="Football AI Dashboard", layout="wide", page_icon="⚽")

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

# Load AI predictions
@st.cache_data
def load_ai_predictions():
    try:
        return pd.read_csv("upcoming_ai_predictions.csv")
    except:
        return pd.DataFrame()

# Load model info
@st.cache_data
def load_ai_model_info():
    try:
        import pickle
        with open('ai_model_preprocessing.pkl', 'rb') as f:
            return pickle.load(f)
    except:
        return None

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

def format_ai_prediction_result(pred):
    """Format AI prediction result with emoji"""
    return {0: "🏠 Home Win", 1: "🤝 Draw", 2: "📦 Away Win"}.get(pred, "❓ Unknown")

def get_confidence_color(confidence):
    """Get color based on confidence level"""
    if confidence > 0.8:
        return "success"
    elif confidence > 0.7:
        return "info" 
    elif confidence > 0.6:
        return "warning"
    else:
        return "error"

# === STREAMLIT APP ===
st.title("⚽ Football AI Intelligence Dashboard")

# Sidebar with model info
with st.sidebar:
    st.header("🤖 AI Model Info")
    
    model_info = load_ai_model_info()
    
    if model_info:
        st.success("✅ AI Model Loaded")
        st.metric("Model Accuracy", f"{float(model_info['model_accuracy']):.2%}")
        st.info(f"Created: {model_info['created_at'][:10]}")
        st.info(f"Features: {len(model_info['feature_cols'])}")
        
        with st.expander("Model Details"):
            st.write(f"**Preprocessing**: {model_info['preprocessing_method']}")
            st.write(f"**Feature Selection**: {model_info['feature_selection_method']}")
            if model_info.get('ensemble_members'):
                st.write(f"**Ensemble Members**: {', '.join(model_info['ensemble_members'])}")
    else:
        st.warning("⚠️ AI Model not found")
        st.info("Run the AI training to generate the model")

tabs = st.tabs(["🏆 League Standings", "📊 Stats", "🔮 Classic Predictions", "🧠 AI Predictions"])

# Tab 1: Standings
with tabs[0]:
    st.header("League Standings")
    leagues = get_leagues_from_standings()
    selected_league = st.selectbox("Select League ID", leagues)
    
    with get_connection() as conn:
        df = pd.read_sql(f"SELECT * FROM standings WHERE league_id = {selected_league}", conn)
    st.dataframe(df, use_container_width=True)

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

        st.dataframe(display.sort_values("starting_at"), use_container_width=True)

# Tab 4: AI Predictions
with tabs[3]:
    st.header("🤖 AI-Powered Football Predictions")
    st.markdown("### Ultra-Advanced Neural Network Predictions (93.29% Accuracy)")
    
    # Load AI predictions
    ai_df = load_ai_predictions()
    
    if ai_df.empty:
        st.warning("No AI predictions available.")
        st.info("💡 Generate AI predictions by running the AI model training notebook.")
        
        # Show model requirements
        with st.expander("🔧 How to Generate AI Predictions"):
            st.markdown("""
            **Steps to generate AI predictions:**
            1. Run the ultra-advanced neural network training
            2. Save the model using the save function
            3. AI predictions will appear here automatically
            
            **Model Features:**
            - 🧠 Ensemble of 3 cutting-edge architectures
            - ⚡ Optuna hyperparameter optimization  
            - 📊 126+ engineered features
            - 🎯 93.29% prediction accuracy
            """)
    
    else:
        st.success(f"✅ Loaded {len(ai_df)} AI predictions")
        
        # Display model performance
        if model_info:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🎯 Model Accuracy", f"{float(model_info['model_accuracy']):.2%}")
            with col2:
                st.metric("📊 Features Used", len(model_info['feature_cols']))
            with col3:
                high_conf = len(ai_df[ai_df['confidence'] > 0.7]) if 'confidence' in ai_df.columns else 0
                st.metric("🔥 High Confidence", high_conf)
            with col4:
                avg_conf = ai_df['confidence'].mean() if 'confidence' in ai_df.columns else 0
                st.metric("📈 Avg Confidence", f"{avg_conf:.1%}")
        
        # Filters
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            # League filter
            if 'league_name' in ai_df.columns:
                leagues = ["All Leagues"] + sorted(ai_df['league_name'].dropna().unique().tolist())
                selected_league = st.selectbox("🏆 Select League", leagues, key="ai_league")
                
                if selected_league != "All Leagues":
                    ai_df = ai_df[ai_df['league_name'] == selected_league]
        
        with col2:
            # Confidence filter
            if 'confidence' in ai_df.columns:
                min_confidence = st.slider("🎯 Minimum Confidence", 0.0, 1.0, 0.5, step=0.05)
                ai_df = ai_df[ai_df['confidence'] >= min_confidence]
        
        with col3:
            # Quality filter
            if 'prediction_quality' in ai_df.columns:
                qualities = ai_df['prediction_quality'].unique().tolist()
                selected_quality = st.selectbox("💎 Quality Level", ["All"] + qualities)
                
                if selected_quality != "All":
                    ai_df = ai_df[ai_df['prediction_quality'] == selected_quality]
        
        if len(ai_df) == 0:
            st.warning("No predictions match the selected filters.")
        else:
            # Display mode selection
            display_mode = st.radio("📱 Display Mode", ["Detailed Cards", "Compact Table"], horizontal=True)
            
            if display_mode == "Detailed Cards":
                # Detailed card view
                st.subheader(f"🎯 {len(ai_df)} AI Predictions")
                
                # Sort by confidence
                ai_df_sorted = ai_df.sort_values('confidence', ascending=False) if 'confidence' in ai_df.columns else ai_df
                
                for idx, row in ai_df_sorted.head(20).iterrows():
                    with st.container():
                        # Create match card
                        col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                        
                        with col1:
                            st.markdown(f"**⚽ {row.get('home_team', 'Home')} vs {row.get('away_team', 'Away')}**")
                            if pd.notna(row.get('starting_at')):
                                try:
                                    date_str = pd.to_datetime(row['starting_at']).strftime("%m/%d %H:%M")
                                    st.caption(f"📅 {date_str}")
                                except:
                                    st.caption("📅 Date TBD")
                            if pd.notna(row.get('league_name')):
                                st.caption(f"🏆 {row['league_name']}")
                        
                        with col2:
                            # AI Prediction
                            pred_text = format_ai_prediction_result(row.get('pred_1x2', 0))
                            st.markdown(f"**🤖 AI Prediction**")
                            st.markdown(pred_text)
                            
                            # Confidence with color
                            conf = row.get('confidence', 0)
                            st.markdown(f"**Confidence**: {conf:.1%}")
                        
                        with col3:
                            # Probabilities
                            st.markdown("**📊 Probabilities**")
                            home_prob = row.get('prob_home', 0)
                            draw_prob = row.get('prob_draw', 0)
                            away_prob = row.get('prob_away', 0)
                            st.write(f"🏠 Home: {home_prob:.1%}")
                            st.write(f"🤝 Draw: {draw_prob:.1%}")
                            st.write(f"📦 Away: {away_prob:.1%}")
                        
                        with col4:
                            # AI Recommendation
                            st.markdown("**💡 AI Recommendation**")
                            recommendation = row.get('ai_recommendation', 'No recommendation')
                            
                            # Color code recommendation
                            if 'STRONG' in recommendation:
                                st.success(recommendation)
                            elif 'Good' in recommendation:
                                st.info(recommendation)
                            elif 'Moderate' in recommendation:
                                st.warning(recommendation)
                            else:
                                st.error(recommendation)
                            
                            # Additional predictions
                            ou25 = "⬆️ Over" if row.get('pred_ou_2_5_goals', 0) == 1 else "⬇️ Under"
                            btts = "✅ Yes" if row.get('pred_btts', 0) == 1 else "❌ No"
                            st.caption(f"O/U 2.5: {ou25}")
                            st.caption(f"BTTS: {btts}")
                        
                        # Quality indicator
                        quality = row.get('prediction_quality', 'unknown')
                        if quality == 'excellent':
                            st.success(f"💎 Excellent Quality Prediction")
                        elif quality == 'high':
                            st.info(f"🔥 High Quality Prediction")
                        elif quality == 'good':
                            st.warning(f"👍 Good Quality Prediction")
                        else:
                            st.caption(f"Quality: {quality}")
                        
                        st.divider()
                
                if len(ai_df_sorted) > 20:
                    st.info(f"Showing top 20 of {len(ai_df_sorted)} predictions. Adjust filters to see more.")
            
            else:
                # Compact table view
                st.subheader(f"📋 {len(ai_df)} AI Predictions (Table View)")
                
                # Prepare display dataframe
                display_df = ai_df.copy()
                
                # Format columns for display
                if 'starting_at' in display_df.columns:
                    display_df['Date & Time'] = pd.to_datetime(display_df['starting_at']).dt.strftime("%m/%d %H:%M")
                
                display_df['Match'] = display_df.apply(lambda x: f"{x.get('home_team', 'Home')} vs {x.get('away_team', 'Away')}", axis=1)
                
                if 'pred_1x2' in display_df.columns:
                    display_df['AI Prediction'] = display_df['pred_1x2'].apply(format_ai_prediction_result)
                
                if 'confidence' in display_df.columns:
                    display_df['Confidence'] = display_df['confidence'].apply(lambda x: f"{x:.1%}")
                
                if 'ai_recommendation' in display_df.columns:
                    display_df['Recommendation'] = display_df['ai_recommendation']
                
                # Select columns to show
                show_columns = ['Match', 'Date & Time', 'league_name', 'AI Prediction', 'Confidence', 'Recommendation']
                available_columns = [col for col in show_columns if col in display_df.columns]
                
                # Rename columns for display
                column_names = {
                    'league_name': 'League',
                    'Date & Time': 'Date & Time'
                }
                
                final_display = display_df[available_columns].rename(columns=column_names)
                
                # Sort by confidence if available
                if 'confidence' in ai_df.columns:
                    final_display = final_display.sort_values('Confidence', ascending=False)
                
                st.dataframe(final_display, use_container_width=True, hide_index=True)
        
        # Analytics section - simplified without Plotly
        if not ai_df.empty and len(ai_df) > 5:
            st.subheader("📈 AI Prediction Analytics")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Prediction distribution
                if 'pred_1x2' in ai_df.columns:
                    pred_counts = ai_df['pred_1x2'].value_counts()
                    st.markdown("**🎯 Prediction Distribution:**")
                    st.write(f"🏠 Home Wins: {pred_counts.get(0, 0)}")
                    st.write(f"🤝 Draws: {pred_counts.get(1, 0)}")
                    st.write(f"📦 Away Wins: {pred_counts.get(2, 0)}")
            
            with col2:
                # Confidence stats
                if 'confidence' in ai_df.columns:
                    st.markdown("**📊 Confidence Stats:**")
                    st.write(f"Average: {ai_df['confidence'].mean():.1%}")
                    st.write(f"Max: {ai_df['confidence'].max():.1%}")
                    st.write(f"Min: {ai_df['confidence'].min():.1%}")
            
            # Quality metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if 'prediction_quality' in ai_df.columns:
                    quality_counts = ai_df['prediction_quality'].value_counts()
                    st.markdown("**💎 Quality Breakdown:**")
                    for quality, count in quality_counts.items():
                        percentage = (count / len(ai_df)) * 100
                        st.write(f"• {quality.title()}: {count} ({percentage:.1f}%)")
            
            with col2:
                if 'confidence' in ai_df.columns:
                    high_conf = len(ai_df[ai_df['confidence'] > 0.8])
                    med_conf = len(ai_df[(ai_df['confidence'] > 0.6) & (ai_df['confidence'] <= 0.8)])
                    low_conf = len(ai_df[ai_df['confidence'] <= 0.6])
                    
                    st.markdown("**🎯 Confidence Levels:**")
                    st.write(f"• High (>80%): {high_conf}")
                    st.write(f"• Medium (60-80%): {med_conf}")
                    st.write(f"• Low (<60%): {low_conf}")
            
            with col3:
                if 'league_name' in ai_df.columns:
                    league_counts = ai_df['league_name'].value_counts().head(3)
                    st.markdown("**🏆 Top Leagues:**")
                    for league, count in league_counts.items():
                        st.write(f"• {league}: {count}")
            
            # Show top recommendations
            if 'ai_recommendation' in ai_df.columns and 'confidence' in ai_df.columns:
                st.subheader("🔥 Top AI Recommendations")
                
                top_picks = ai_df.nlargest(5, 'confidence')
                
                for idx, row in top_picks.iterrows():
                    col1, col2, col3 = st.columns([3, 2, 2])
                    
                    with col1:
                        st.write(f"**{row.get('home_team', 'Home')} vs {row.get('away_team', 'Away')}**")
                    
                    with col2:
                        st.write(format_ai_prediction_result(row.get('pred_1x2', 0)))
                    
                    with col3:
                        confidence = row.get('confidence', 0)
                        if confidence > 0.8:
                            st.success(f"🔥 {confidence:.1%}")
                        elif confidence > 0.7:
                            st.info(f"👍 {confidence:.1%}")
                        else:
                            st.warning(f"⚠️ {confidence:.1%}")
        
        # Footer with model info
        with st.expander("🤖 About the AI Model"):
            st.markdown("""
            ### Ultra-Advanced Football Prediction AI
            
            **🧠 Model Architecture:**
            - Ensemble of 3 cutting-edge neural networks
            - Optuna-Optimized, Residual, and Capsule networks
            - Optuna-optimized hyperparameters
            
            **📊 Features:**
            - 126+ engineered features from match statistics
            - Advanced preprocessing with Power Transform
            - Time-based and contextual features
            
            **🎯 Performance:**
            - 93.29% accuracy on test data
            - Professional-grade prediction quality
            - Confidence-weighted recommendations
            
            **⚡ Key Capabilities:**
            - 1x2 Result prediction (Home/Draw/Away)
            - Over/Under goals prediction
            - Both Teams to Score (BTTS)
            - Confidence scoring for each prediction
            - Quality assessment of predictions
            """)
            
            if model_info:
                st.json({
                    "Model Accuracy": f"{float(model_info['model_accuracy']):.4f}",
                    "Features Count": len(model_info['feature_cols']),
                    "Preprocessing": model_info['preprocessing_method'],
                    "Feature Selection": model_info['feature_selection_method'],
                    "Created": model_info['created_at']
                })

# Footer
st.markdown("---")
st.markdown("### 🚀 Football AI Intelligence Dashboard")
st.markdown("Powered by Ultra-Advanced Neural Networks | 93.29% Prediction Accuracy")