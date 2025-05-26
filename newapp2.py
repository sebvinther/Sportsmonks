import streamlit as st
import pickle
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="⚽ Football Betting AI Predictor",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-metric {
        background-color: #d4edda;
        border-left-color: #28a745;
    }
    .warning-metric {
        background-color: #fff3cd;
        border-left-color: #ffc107;
    }
    .danger-metric {
        background-color: #f8d7da;
        border-left-color: #dc3545;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_models_and_data():
    """Load all trained models and data with error handling"""
    try:
        # Load trained models with XGBoost error handling
        with open('trained_models.pkl', 'rb') as f:
            trained_models = pickle.load(f)
        
        # Load model metadata
        with open('model_metadata.pkl', 'rb') as f:
            model_metadata = pickle.load(f)
        
        # Load streamlit summary
        with open('streamlit_summary.pkl', 'rb') as f:
            streamlit_summary = pickle.load(f)
        
        # Load ML data
        with open('ml_data_prepared.pkl', 'rb') as f:
            ml_data = pickle.load(f)
        
        return trained_models, model_metadata, streamlit_summary, ml_data
    
    except Exception as e:
        if "XGBoost" in str(e) or "libxgboost" in str(e):
            st.error("❌ XGBoost Library Error Detected")
            st.error("Please run: `brew install libomp` in your terminal to fix this.")
            st.write("**Alternative solutions:**")
            st.write("1. `pip uninstall xgboost && pip install xgboost`")
            st.write("2. Install Homebrew if not installed, then run `brew install libomp`")
            
            # Create mock data for demo purposes
            st.warning("🎭 **DEMO MODE**: Using mock data for demonstration")
            return create_demo_data()
        else:
            st.error(f"Error loading files: {e}")
            st.error("Please ensure you've run the model training steps first.")
            return None, None, None, None

def create_demo_data():
    """Create demo data when models can't be loaded"""
    
    # Mock trained models (non-functional for demo)
    trained_models = {
        'xgboost': {'model': None, 'results': {'feature_importance': None}, 'type': 'tree'},
        'svm': {'model': None, 'results': {}, 'type': 'svm'},
        'random_forest': {'model': None, 'results': {'feature_importance': None}, 'type': 'tree'},
        'neural_network': {'model': None, 'results': {}, 'type': 'neural'}
    }
    
    # Mock model metadata
    import pandas as pd
    models_comparison = pd.DataFrame({
        'Model': ['XGBoost', 'Random Forest', 'SVM', 'Neural Network'],
        'Test_Accuracy': [0.6397, 0.6055, 0.5836, 0.5762],
        'Train_Accuracy': [0.7234, 0.6891, 0.6123, 0.6012],
        'CV_Mean': [0.6301, 0.6021, 0.5798, 0.5734],
        'CV_Std': [0.0123, 0.0156, 0.0189, 0.0201],
        'Overfitting': [0.0837, 0.0836, 0.0287, 0.0250]
    })
    
    feature_names = [
        'h2h_games_played', 'h2h_home_wins', 'h2h_draws', 'h2h_away_wins',
        'home_form_5_goals_for_avg', 'away_form_5_goals_for_avg',
        'home_form_5_goals_against_avg', 'away_form_5_goals_against_avg',
        'league_avg_total_goals', 'season_avg_home_win'
    ] + [f'feature_{i}' for i in range(76)]  # 86 total features
    
    model_metadata = {
        'best_model': 'xgboost',
        'feature_names': feature_names,
        'label_encoder': None,
        'scaler': None,
        'models_comparison': models_comparison,
        'betting_results': {
            'total_bets': 2023,
            'winning_bets': 1657,
            'win_rate': 0.819,
            'total_profit': 33036.0,
            'roi': 81.65,
            'final_bankroll': 34036.0,
            'bet_history': []
        },
        'baseline_accuracy': 0.442,
        'training_samples': 16248,
        'test_samples': 4061
    }
    
    streamlit_summary = {
        'best_model': 'XGBoost',
        'best_accuracy': 0.6397,
        'total_features': 86,
        'betting_roi': 81.65,
        'betting_win_rate': 0.819
    }
    
    ml_data = {'demo': True}
    
    return trained_models, model_metadata, streamlit_summary, ml_data

@st.cache_data
def load_team_data():
    """Load team data from database or CSV"""
    try:
        # Try to load from database first
        db_path = '/Users/sebastianvinther/Desktop/Sportsmonks/db_sportmonks.db'
        conn = sqlite3.connect(db_path)
        
        teams_query = """
        SELECT DISTINCT 
            t.id as team_id,
            t.name as team_name,
            c.name as country,
            l.name as league
        FROM teams t
        LEFT JOIN countries c ON t.country_id = c.id
        LEFT JOIN fixtures f ON (t.id = f.home_team_id OR t.id = f.away_team_id)
        LEFT JOIN leagues l ON f.league_id = l.id
        WHERE t.name IS NOT NULL
        ORDER BY t.name
        """
        
        teams_df = pd.read_sql_query(teams_query, conn)
        conn.close()
        
        return teams_df
    
    except:
        # Fallback to sample data if database not available
        return pd.DataFrame({
            'team_id': range(1, 21),
            'team_name': [
                'Arsenal', 'Chelsea', 'Liverpool', 'Manchester City', 'Manchester United',
                'Tottenham', 'Newcastle', 'Brighton', 'Aston Villa', 'West Ham',
                'Crystal Palace', 'Fulham', 'Wolves', 'Everton', 'Brentford',
                'Nottingham Forest', 'Luton Town', 'Burnley', 'Sheffield United', 'Bournemouth'
            ],
            'country': ['England'] * 20,
            'league': ['Premier League'] * 20
        })

def create_prediction_input_form(teams_df):
    """Create form for match prediction input"""
    st.subheader("🎯 Match Prediction")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🏠 Home Team**")
        home_team = st.selectbox(
            "Select Home Team",
            teams_df['team_name'].unique(),
            key='home_team'
        )
        
        # Home team recent form (mock data for demo)
        st.markdown("**Recent Form (Last 5 games)**")
        home_form_goals = st.slider("Avg Goals Scored", 0.0, 5.0, 1.5, 0.1, key='home_goals')
        home_form_conceded = st.slider("Avg Goals Conceded", 0.0, 5.0, 1.2, 0.1, key='home_conceded')
        home_possession = st.slider("Avg Possession %", 30.0, 80.0, 55.0, 1.0, key='home_poss')
    
    with col2:
        st.markdown("**✈️ Away Team**")
        away_team = st.selectbox(
            "Select Away Team",
            teams_df['team_name'].unique(),
            key='away_team'
        )
        
        # Away team recent form (mock data for demo)
        st.markdown("**Recent Form (Last 5 games)**")
        away_form_goals = st.slider("Avg Goals Scored", 0.0, 5.0, 1.3, 0.1, key='away_goals')
        away_form_conceded = st.slider("Avg Goals Conceded", 0.0, 5.0, 1.4, 0.1, key='away_conceded')
        away_possession = st.slider("Avg Possession %", 30.0, 80.0, 45.0, 1.0, key='away_poss')
    
    # Additional match context
    st.markdown("**📅 Match Context**")
    col3, col4, col5 = st.columns(3)
    
    with col3:
        match_date = st.date_input("Match Date", datetime.now().date())
    
    with col4:
        league = st.selectbox("League", ["Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"])
    
    with col5:
        importance = st.selectbox("Match Importance", ["Regular", "Derby", "European Competition", "Final"])
    
    return {
        'home_team': home_team,
        'away_team': away_team,
        'home_form_goals': home_form_goals,
        'home_form_conceded': home_form_conceded,
        'home_possession': home_possession,
        'away_form_goals': away_form_goals,
        'away_form_conceded': away_form_conceded,
        'away_possession': away_possession,
        'match_date': match_date,
        'league': league,
        'importance': importance
    }

def create_mock_features(match_input, model_metadata):
    """Create mock feature vector for prediction (in real app, this would use actual data)"""
    feature_names = model_metadata['feature_names']
    
    # Create feature vector with mock values based on user input
    features = np.zeros(len(feature_names))
    
    # Fill some features based on input
    goal_diff = match_input['home_form_goals'] - match_input['away_form_goals']
    defensive_diff = match_input['away_form_conceded'] - match_input['home_form_conceded']
    possession_diff = match_input['home_possession'] - match_input['away_possession']
    
    # Mock feature mapping (in real app, this would be proper feature engineering)
    for i, feature in enumerate(feature_names):
        if 'goal_diff_advantage' in feature:
            features[i] = goal_diff
        elif 'home_form' in feature and 'goals_for' in feature:
            features[i] = match_input['home_form_goals']
        elif 'away_form' in feature and 'goals_for' in feature:
            features[i] = match_input['away_form_goals']
        elif 'home_form' in feature and 'goals_against' in feature:
            features[i] = match_input['home_form_conceded']
        elif 'away_form' in feature and 'goals_against' in feature:
            features[i] = match_input['away_form_conceded']
        elif 'possession' in feature and 'home' in feature:
            features[i] = match_input['home_possession']
        elif 'possession' in feature and 'away' in feature:
            features[i] = match_input['away_possession']
        elif 'league_avg' in feature:
            features[i] = np.random.normal(0.5, 0.1)  # Mock league averages
        elif 'h2h' in feature:
            features[i] = np.random.normal(0.3, 0.2)  # Mock head-to-head
        else:
            features[i] = np.random.normal(0, 0.5)  # Mock other features
    
    return features.reshape(1, -1)

def make_prediction(match_input, trained_models, model_metadata):
    """Make prediction using the best model or demo prediction"""
    
    # Check if in demo mode
    if model_metadata.get('demo_mode') or trained_models['xgboost']['model'] is None:
        return make_demo_prediction(match_input)
    
    # Create feature vector
    features = create_mock_features(match_input, model_metadata)
    
    # Scale features
    scaler = model_metadata['scaler']
    features_scaled = scaler.transform(features)
    
    # Get best model
    best_model_name = model_metadata['best_model']
    best_model = trained_models[best_model_name]['model']
    
    # Make prediction
    if best_model_name in ['svm', 'neural_network']:
        prediction = best_model.predict(features_scaled)[0]
        probabilities = best_model.predict_proba(features_scaled)[0]
    else:
        prediction = best_model.predict(features)[0]
        probabilities = best_model.predict_proba(features)[0]
    
    # Convert back to labels
    le = model_metadata['label_encoder']
    predicted_outcome = le.inverse_transform([prediction])[0]
    
    # Create probability dict
    prob_dict = {}
    for i, class_label in enumerate(le.classes_):
        prob_dict[class_label] = probabilities[i]
    
    return predicted_outcome, prob_dict, probabilities.max()

def make_demo_prediction(match_input):
    """Make demo prediction when models aren't available"""
    
    # Calculate mock prediction based on team form
    home_strength = (match_input['home_form_goals'] - match_input['home_form_conceded'] + 
                    match_input['home_possession'] / 100)
    away_strength = (match_input['away_form_goals'] - match_input['away_form_conceded'] + 
                    match_input['away_possession'] / 100)
    
    # Add home advantage
    home_strength += 0.3
    
    # Calculate probabilities
    total_strength = home_strength + away_strength + 1.0  # +1 for draw probability
    
    prob_home = max(0.1, min(0.8, home_strength / total_strength + np.random.normal(0, 0.1)))
    prob_away = max(0.1, min(0.8, away_strength / total_strength + np.random.normal(0, 0.1)))
    prob_draw = max(0.1, 1.0 - prob_home - prob_away)
    
    # Normalize probabilities
    total_prob = prob_home + prob_draw + prob_away
    prob_home /= total_prob
    prob_draw /= total_prob
    prob_away /= total_prob
    
    # Determine prediction
    probs = [prob_home, prob_draw, prob_away]
    outcomes = ['H', 'D', 'A']
    predicted_outcome = outcomes[np.argmax(probs)]
    
    prob_dict = {'H': prob_home, 'D': prob_draw, 'A': prob_away}
    confidence = max(probs)
    
    return predicted_outcome, prob_dict, confidence

def display_prediction_results(predicted_outcome, prob_dict, confidence, match_input):
    """Display prediction results with betting recommendations"""
    
    st.subheader("🎯 Prediction Results")
    
    # Main prediction
    col1, col2, col3 = st.columns(3)
    
    outcome_map = {'H': 'Home Win', 'D': 'Draw', 'A': 'Away Win'}
    emoji_map = {'H': '🏠', 'D': '🤝', 'A': '✈️'}
    
    with col1:
        st.metric(
            label="Predicted Outcome",
            value=f"{emoji_map[predicted_outcome]} {outcome_map[predicted_outcome]}"
        )
    
    with col2:
        st.metric(
            label="Confidence",
            value=f"{confidence:.1%}"
        )
    
    with col3:
        confidence_level = "High" if confidence > 0.6 else "Medium" if confidence > 0.45 else "Low"
        color = "success" if confidence > 0.6 else "warning" if confidence > 0.45 else "danger"
        st.metric(
            label="Confidence Level",
            value=confidence_level
        )
    
    # Probability breakdown
    st.subheader("📊 Probability Breakdown")
    
    prob_df = pd.DataFrame([
        {'Outcome': '🏠 Home Win', 'Probability': prob_dict['H'], 'Percentage': f"{prob_dict['H']:.1%}"},
        {'Outcome': '🤝 Draw', 'Probability': prob_dict['D'], 'Percentage': f"{prob_dict['D']:.1%}"},
        {'Outcome': '✈️ Away Win', 'Probability': prob_dict['A'], 'Percentage': f"{prob_dict['A']:.1%}"}
    ])
    
    # Create probability chart
    fig = px.bar(
        prob_df, 
        x='Outcome', 
        y='Probability',
        title=f"Match Prediction: {match_input['home_team']} vs {match_input['away_team']}",
        color='Probability',
        color_continuous_scale='RdYlGn'
    )
    fig.update_layout(showlegend=False, height=400)
    st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    # Betting recommendation
    st.subheader("💰 Betting Recommendation")
    
    # Mock odds (in real app, these would come from bookmakers)
    mock_odds = {'H': 2.1, 'D': 3.2, 'A': 2.8}
    
    # Calculate expected value
    expected_values = {}
    for outcome, prob in prob_dict.items():
        implied_prob = 1 / mock_odds[outcome]
        expected_value = (prob * mock_odds[outcome]) - 1
        expected_values[outcome] = expected_value
    
    best_bet = max(expected_values.items(), key=lambda x: x[1])
    
    if best_bet[1] > 0.05 and confidence > 0.55:  # Positive expected value and reasonable confidence
        st.success(f"✅ **RECOMMENDED BET**: {outcome_map[best_bet[0]]} at odds {mock_odds[best_bet[0]]}")
        st.write(f"Expected Value: +{best_bet[1]:.1%}")
        st.write(f"Recommended stake: 2-5% of bankroll")
    else:
        st.warning("⚠️ **NO BET RECOMMENDED**: No significant value detected")
        st.write("Wait for better opportunities with higher expected value")

def display_model_performance(model_metadata):
    """Display model performance comparison"""
    st.header("🤖 Model Performance Comparison")
    
    models_comparison = model_metadata['models_comparison']
    
    # Performance metrics
    col1, col2, col3, col4 = st.columns(4)
    
    best_model = models_comparison.iloc[0]
    
    with col1:
        st.metric(
            label="🥇 Best Model",
            value=best_model['Model']
        )
    
    with col2:
        st.metric(
            label="📊 Accuracy",
            value=f"{best_model['Test_Accuracy']:.3f}"
        )
    
    with col3:
        baseline = model_metadata['baseline_accuracy']
        improvement = best_model['Test_Accuracy'] - baseline
        st.metric(
            label="📈 Improvement",
            value=f"+{improvement:.3f}",
            delta=f"{improvement/baseline:.1%}"
        )
    
    with col4:
        overfitting = best_model['Overfitting']
        st.metric(
            label="🎯 Overfitting",
            value=f"{overfitting:.3f}",
            delta="Low" if overfitting < 0.05 else "High"
        )
    
    # Model comparison chart
    fig = px.bar(
        models_comparison,
        x='Model',
        y='Test_Accuracy',
        title="Model Accuracy Comparison",
        color='Test_Accuracy',
        color_continuous_scale='RdYlGn'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    # Detailed comparison table
    st.subheader("📋 Detailed Model Metrics")
    
    display_df = models_comparison.copy()
    display_df['Test_Accuracy'] = display_df['Test_Accuracy'].round(4)
    display_df['Train_Accuracy'] = display_df['Train_Accuracy'].round(4)
    display_df['CV_Mean'] = display_df['CV_Mean'].round(4)
    display_df['CV_Std'] = display_df['CV_Std'].round(4)
    display_df['Overfitting'] = display_df['Overfitting'].round(4)
    
    st.dataframe(display_df, use_container_width=True)

def display_betting_analysis(model_metadata):
    """Display betting performance analysis"""
    st.header("💰 Betting Performance Analysis")
    
    betting_results = model_metadata['betting_results']
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="🎯 Total Bets",
            value=f"{betting_results['total_bets']:,}"
        )
    
    with col2:
        st.metric(
            label="🏆 Win Rate",
            value=f"{betting_results['win_rate']:.1%}"
        )
    
    with col3:
        roi = betting_results['roi']
        st.metric(
            label="📈 ROI",
            value=f"{roi:.1f}%",
            delta="Profitable!" if roi > 0 else "Loss"
        )
    
    with col4:
        st.metric(
            label="💵 Total Profit",
            value=f"${betting_results['total_profit']:,.0f}"
        )
    
    # ROI breakdown
    if betting_results['roi'] > 0:
        st.success("🎉 **PROFITABLE BETTING STRATEGY ACHIEVED!**")
        st.write("This model shows strong potential for profitable betting when used with proper bankroll management.")
    else:
        st.warning("📊 **Strategy needs refinement**")
        st.write("Consider adjusting confidence thresholds or focusing on specific bet types.")
    
    # Simulate betting growth
    st.subheader("📈 Bankroll Growth Simulation")
    
    # Mock data for betting history visualization
    bet_history = betting_results.get('bet_history', [])
    
    if bet_history:
        cumulative_profit = np.cumsum([bet['profit'] for bet in bet_history])
        betting_dates = pd.date_range(start='2024-01-01', periods=len(bet_history), freq='D')
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=betting_dates,
            y=1000 + cumulative_profit,  # Starting bankroll of $1000
            mode='lines',
            name='Bankroll',
            line=dict(color='green', width=3)
        ))
        fig.update_layout(
            title="Bankroll Growth Over Time",
            xaxis_title="Date",
            yaxis_title="Bankroll ($)",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    else:
        # Create mock visualization
        days = 100
        mock_returns = np.random.normal(0.02, 0.1, days)  # Mock daily returns
        cumulative_returns = np.cumprod(1 + mock_returns)
        mock_bankroll = 1000 * cumulative_returns
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pd.date_range(start='2024-01-01', periods=days, freq='D'),
            y=mock_bankroll,
            mode='lines',
            name='Bankroll Growth',
            line=dict(color='green', width=3)
        ))
        fig.update_layout(
            title="Simulated Bankroll Growth",
            xaxis_title="Date",
            yaxis_title="Bankroll ($)",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")

def get_team_statistics():
    """Get team statistics data for punters"""
    try:
        # Try to load from database
        db_path = '/Users/sebastianvinther/Desktop/Sportsmonks/db_sportmonks.db'
        conn = sqlite3.connect(db_path)
        
        # Get comprehensive team stats
        team_stats_query = """
        SELECT 
            t.name as team_name,
            l.name as league,
            COUNT(DISTINCT f.id) as matches_played,
            
            -- Goal statistics
            AVG(CASE WHEN f.home_team_id = t.id THEN f.score_home 
                     WHEN f.away_team_id = t.id THEN f.score_away END) as avg_goals_scored,
            AVG(CASE WHEN f.home_team_id = t.id THEN f.score_away 
                     WHEN f.away_team_id = t.id THEN f.score_home END) as avg_goals_conceded,
            
            -- Win rates
            AVG(CASE WHEN (f.home_team_id = t.id AND f.score_home > f.score_away) OR 
                          (f.away_team_id = t.id AND f.score_away > f.score_home) 
                     THEN 1.0 ELSE 0.0 END) as win_rate,
            
            -- Home/Away splits
            AVG(CASE WHEN f.home_team_id = t.id AND f.score_home > f.score_away THEN 1.0 
                     WHEN f.home_team_id = t.id THEN 0.0 END) as home_win_rate,
            AVG(CASE WHEN f.away_team_id = t.id AND f.score_away > f.score_home THEN 1.0 
                     WHEN f.away_team_id = t.id THEN 0.0 END) as away_win_rate,
            
            -- Over/Under metrics
            AVG(CASE WHEN (COALESCE(f.score_home, 0) + COALESCE(f.score_away, 0)) > 2.5 
                     THEN 1.0 ELSE 0.0 END) as over_2_5_rate,
            AVG(CASE WHEN f.score_home > 0 AND f.score_away > 0 THEN 1.0 ELSE 0.0 END) as btts_rate,
            
            -- Recent form (last 10 games)
            AVG(CASE WHEN f.starting_at >= date('now', '-60 days') THEN
                CASE WHEN (f.home_team_id = t.id AND f.score_home > f.score_away) OR 
                          (f.away_team_id = t.id AND f.score_away > f.score_home) 
                     THEN 1.0 ELSE 0.0 END END) as recent_form
            
        FROM teams t
        LEFT JOIN fixtures f ON (t.id = f.home_team_id OR t.id = f.away_team_id)
        LEFT JOIN leagues l ON f.league_id = l.id
        WHERE f.score_home IS NOT NULL 
        AND f.score_away IS NOT NULL
        AND f.starting_at >= date('now', '-2 years')
        GROUP BY t.id, t.name, l.name
        HAVING matches_played >= 10
        ORDER BY win_rate DESC
        """
        
        team_stats_df = pd.read_sql_query(team_stats_query, conn)
        conn.close()
        
        # Calculate additional metrics
        team_stats_df['goal_difference'] = team_stats_df['avg_goals_scored'] - team_stats_df['avg_goals_conceded']
        team_stats_df['points_per_game'] = team_stats_df['win_rate'] * 3 + (1 - team_stats_df['win_rate'] - 
            (team_stats_df['avg_goals_scored'] < team_stats_df['avg_goals_conceded']).astype(float)) * 1
        
        return team_stats_df
    
    except Exception as e:
        # Return mock data if database fails
        return create_mock_team_stats()

def create_mock_team_stats():
    """Create mock team statistics for demo"""
    teams = ['Manchester City', 'Arsenal', 'Liverpool', 'Chelsea', 'Manchester United', 
             'Newcastle', 'Tottenham', 'Brighton', 'Aston Villa', 'West Ham']
    
    mock_data = []
    for i, team in enumerate(teams):
        mock_data.append({
            'team_name': team,
            'league': 'Premier League',
            'matches_played': np.random.randint(25, 35),
            'avg_goals_scored': round(np.random.uniform(1.2, 2.8), 2),
            'avg_goals_conceded': round(np.random.uniform(0.8, 2.2), 2),
            'win_rate': round(np.random.uniform(0.3, 0.8), 3),
            'home_win_rate': round(np.random.uniform(0.4, 0.9), 3),
            'away_win_rate': round(np.random.uniform(0.2, 0.7), 3),
            'over_2_5_rate': round(np.random.uniform(0.4, 0.7), 3),
            'btts_rate': round(np.random.uniform(0.4, 0.8), 3),
            'recent_form': round(np.random.uniform(0.2, 0.9), 3)
        })
    
    df = pd.DataFrame(mock_data)
    df['goal_difference'] = df['avg_goals_scored'] - df['avg_goals_conceded']
    df['points_per_game'] = df['win_rate'] * 3
    return df.round(3)

def display_team_statistics():
    """Display comprehensive team statistics for punters"""
    st.header("⚽ Team Statistics Dashboard")
    st.write("Comprehensive team performance metrics for informed betting decisions")
    
    # Load team statistics
    team_stats_df = get_team_statistics()
    
    if team_stats_df.empty:
        st.error("No team statistics available")
        return
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        selected_leagues = st.multiselect(
            "Filter by League:",
            options=team_stats_df['league'].unique(),
            default=team_stats_df['league'].unique()[:3] if len(team_stats_df['league'].unique()) > 3 else team_stats_df['league'].unique()
        )
    
    with col2:
        min_matches = st.slider("Minimum matches played:", 5, 50, 10)
    
    # Filter data
    filtered_df = team_stats_df[
        (team_stats_df['league'].isin(selected_leagues)) & 
        (team_stats_df['matches_played'] >= min_matches)
    ]
    
    # Key metrics overview
    st.subheader("📊 League Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Teams Analyzed", len(filtered_df))
    with col2:
        st.metric("Avg Goals/Game", f"{filtered_df['avg_goals_scored'].mean():.2f}")
    with col3:
        st.metric("Avg Over 2.5 Rate", f"{filtered_df['over_2_5_rate'].mean():.1%}")
    with col4:
        st.metric("Avg BTTS Rate", f"{filtered_df['btts_rate'].mean():.1%}")
    
    # Performance charts
    tab1, tab2, tab3 = st.tabs(["🎯 Attacking vs Defensive", "🏠 Home vs Away", "📈 Betting Metrics"])
    
    with tab1:
        # Attacking vs Defensive scatter plot
        fig = px.scatter(
            filtered_df,
            x='avg_goals_conceded',
            y='avg_goals_scored',
            color='win_rate',
            size='matches_played',
            hover_name='team_name',
            title="Team Attacking vs Defensive Performance",
            labels={
                'avg_goals_conceded': 'Average Goals Conceded',
                'avg_goals_scored': 'Average Goals Scored',
                'win_rate': 'Win Rate'
            },
            color_continuous_scale='RdYlGn'
        )
        fig.add_hline(y=filtered_df['avg_goals_scored'].mean(), line_dash="dash", 
                     annotation_text="League Average Goals Scored")
        fig.add_vline(x=filtered_df['avg_goals_conceded'].mean(), line_dash="dash",
                     annotation_text="League Average Goals Conceded")
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    with tab2:
        # Home vs Away performance
        fig = px.scatter(
            filtered_df,
            x='away_win_rate',
            y='home_win_rate',
            color='goal_difference',
            size='matches_played',
            hover_name='team_name',
            title="Home vs Away Performance",
            labels={
                'away_win_rate': 'Away Win Rate',
                'home_win_rate': 'Home Win Rate',
                'goal_difference': 'Goal Difference'
            },
            color_continuous_scale='RdYlGn'
        )
        # Add diagonal line for balanced teams
        fig.add_shape(type="line", x0=0, y0=0, x1=1, y1=1, line=dict(dash="dash"))
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    with tab3:
        # Betting-focused metrics
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                filtered_df.nlargest(10, 'over_2_5_rate'),
                x='over_2_5_rate',
                y='team_name',
                title="Top 10 Teams - Over 2.5 Goals Rate",
                orientation='h',
                color='over_2_5_rate',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
        
        with col2:
            fig = px.bar(
                filtered_df.nlargest(10, 'btts_rate'),
                x='btts_rate',
                y='team_name',
                title="Top 10 Teams - Both Teams to Score Rate",
                orientation='h',
                color='btts_rate',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    # Detailed statistics table
    st.subheader("📋 Detailed Team Statistics")
    
    # Format the dataframe for display
    display_df = filtered_df.copy()
    display_df['avg_goals_scored'] = display_df['avg_goals_scored'].round(2)
    display_df['avg_goals_conceded'] = display_df['avg_goals_conceded'].round(2)
    display_df['win_rate'] = (display_df['win_rate'] * 100).round(1)
    display_df['home_win_rate'] = (display_df['home_win_rate'] * 100).round(1)
    display_df['away_win_rate'] = (display_df['away_win_rate'] * 100).round(1)
    display_df['over_2_5_rate'] = (display_df['over_2_5_rate'] * 100).round(1)
    display_df['btts_rate'] = (display_df['btts_rate'] * 100).round(1)
    display_df['recent_form'] = (display_df['recent_form'] * 100).round(1)
    
    # Rename columns for display
    display_df = display_df.rename(columns={
        'team_name': 'Team',
        'league': 'League',
        'matches_played': 'Matches',
        'avg_goals_scored': 'Goals For',
        'avg_goals_conceded': 'Goals Against',
        'win_rate': 'Win Rate (%)',
        'home_win_rate': 'Home Win (%)',
        'away_win_rate': 'Away Win (%)',
        'over_2_5_rate': 'Over 2.5 (%)',
        'btts_rate': 'BTTS (%)',
        'recent_form': 'Recent Form (%)',
        'goal_difference': 'Goal Diff'
    })
    
    st.dataframe(
        display_df[['Team', 'League', 'Matches', 'Goals For', 'Goals Against', 
                   'Goal Diff', 'Win Rate (%)', 'Home Win (%)', 'Away Win (%)', 
                   'Over 2.5 (%)', 'BTTS (%)', 'Recent Form (%)']],
        use_container_width=True
    )

def get_player_statistics():
    """Get player statistics data for punters"""
    try:
        # Try to load from database
        db_path = '/Users/sebastianvinther/Desktop/Sportsmonks/db_sportmonks.db'
        conn = sqlite3.connect(db_path)
        
        # Get player stats with team and league info
        player_stats_query = """
        SELECT 
            p.common_name as player_name,
            p.position,
            t.name as team_name,
            l.name as league,
            COUNT(DISTINCT ps.fixture_id) as appearances,
            
            -- Goal statistics
            SUM(CASE WHEN ps.type = 'Goals' THEN CAST(ps.value AS INTEGER) ELSE 0 END) as total_goals,
            SUM(CASE WHEN ps.type = 'Assists' THEN CAST(ps.value AS INTEGER) ELSE 0 END) as total_assists,
            
            -- Cards
            SUM(CASE WHEN ps.type = 'Yellowcards' THEN CAST(ps.value AS INTEGER) ELSE 0 END) as yellow_cards,
            SUM(CASE WHEN ps.type = 'Redcards' THEN CAST(ps.value AS INTEGER) ELSE 0 END) as red_cards,
            
            -- Performance metrics
            AVG(CASE WHEN ps.type = 'Rating' THEN CAST(ps.value AS REAL) END) as avg_rating,
            AVG(CASE WHEN ps.type = 'Minutes Played' THEN CAST(ps.value AS REAL) END) as avg_minutes,
            
            -- Passing
            AVG(CASE WHEN ps.type = 'Passes' THEN CAST(ps.value AS INTEGER) END) as avg_passes,
            AVG(CASE WHEN ps.type = 'Accurate Passes Percentage' THEN CAST(ps.value AS REAL) END) as pass_accuracy
            
        FROM players p
        JOIN player_statistics ps ON p.id = ps.player_id
        JOIN fixtures f ON ps.fixture_id = f.id
        JOIN teams t ON ps.team_id = t.id
        JOIN leagues l ON f.league_id = l.id
        WHERE f.starting_at >= date('now', '-1 year')
        AND ps.value IS NOT NULL
        AND ps.value != ''
        GROUP BY p.id, p.common_name, p.position, t.name, l.name
        HAVING appearances >= 5
        ORDER BY total_goals DESC
        LIMIT 200
        """
        
        player_stats_df = pd.read_sql_query(player_stats_query, conn)
        conn.close()
        
        # Calculate per-game statistics
        player_stats_df['goals_per_game'] = player_stats_df['total_goals'] / player_stats_df['appearances']
        player_stats_df['assists_per_game'] = player_stats_df['total_assists'] / player_stats_df['appearances']
        player_stats_df['cards_per_game'] = (player_stats_df['yellow_cards'] + player_stats_df['red_cards']) / player_stats_df['appearances']
        
        return player_stats_df
    
    except Exception as e:
        # Return mock data if database fails
        return create_mock_player_stats()

def create_mock_player_stats():
    """Create mock player statistics for demo"""
    players_data = [
        {"player_name": "Erling Haaland", "position": "Forward", "team_name": "Manchester City", "league": "Premier League"},
        {"player_name": "Mohamed Salah", "position": "Forward", "team_name": "Liverpool", "league": "Premier League"},
        {"player_name": "Harry Kane", "position": "Forward", "team_name": "Bayern Munich", "league": "Bundesliga"},
        {"player_name": "Kylian Mbappe", "position": "Forward", "team_name": "PSG", "league": "Ligue 1"},
        {"player_name": "Kevin De Bruyne", "position": "Midfielder", "team_name": "Manchester City", "league": "Premier League"},
        {"player_name": "Bruno Fernandes", "position": "Midfielder", "team_name": "Manchester United", "league": "Premier League"},
        {"player_name": "Virgil van Dijk", "position": "Defender", "team_name": "Liverpool", "league": "Premier League"},
        {"player_name": "Ruben Dias", "position": "Defender", "team_name": "Manchester City", "league": "Premier League"},
        {"player_name": "Alisson", "position": "Goalkeeper", "team_name": "Liverpool", "league": "Premier League"},
        {"player_name": "Ederson", "position": "Goalkeeper", "team_name": "Manchester City", "league": "Premier League"},
    ]
    
    mock_data = []
    for player in players_data:
        if player["position"] == "Forward":
            goals_range = (15, 35)
            assists_range = (3, 12)
        elif player["position"] == "Midfielder":
            goals_range = (5, 20)
            assists_range = (8, 20)
        elif player["position"] == "Defender":
            goals_range = (1, 8)
            assists_range = (1, 8)
        else:  # Goalkeeper
            goals_range = (0, 1)
            assists_range = (0, 3)
        
        appearances = np.random.randint(20, 35)
        total_goals = np.random.randint(*goals_range)
        total_assists = np.random.randint(*assists_range)
        
        mock_data.append({
            **player,
            'appearances': appearances,
            'total_goals': total_goals,
            'total_assists': total_assists,
            'yellow_cards': np.random.randint(2, 8),
            'red_cards': np.random.randint(0, 2),
            'avg_rating': round(np.random.uniform(6.5, 8.5), 2),
            'avg_minutes': round(np.random.uniform(70, 90), 1),
            'avg_passes': np.random.randint(30, 80),
            'pass_accuracy': round(np.random.uniform(75, 95), 1),
            'goals_per_game': round(total_goals / appearances, 2),
            'assists_per_game': round(total_assists / appearances, 2),
            'cards_per_game': round(np.random.uniform(0.1, 0.4), 2)
        })
    
    return pd.DataFrame(mock_data)

def display_player_statistics():
    """Display comprehensive player statistics for punters"""
    st.header("👤 Player Statistics Dashboard")
    st.write("Individual player performance metrics for player-specific betting markets")
    
    # Load player statistics
    player_stats_df = get_player_statistics()
    
    if player_stats_df.empty:
        st.error("No player statistics available")
        return
    
    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_leagues = st.multiselect(
            "Filter by League:",
            options=player_stats_df['league'].unique(),
            default=player_stats_df['league'].unique()[:2] if len(player_stats_df['league'].unique()) > 2 else player_stats_df['league'].unique()
        )
    
    with col2:
        selected_positions = st.multiselect(
            "Filter by Position:",
            options=player_stats_df['position'].unique(),
            default=player_stats_df['position'].unique()
        )
    
    with col3:
        min_appearances = st.slider("Minimum appearances:", 1, 30, 5)
    
    # Filter data
    filtered_df = player_stats_df[
        (player_stats_df['league'].isin(selected_leagues)) & 
        (player_stats_df['position'].isin(selected_positions)) &
        (player_stats_df['appearances'] >= min_appearances)
    ]
    
    if filtered_df.empty:
        st.warning("No players match the current filters")
        return
    
    # Key metrics overview
    st.subheader("📊 Player Performance Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Players Analyzed", len(filtered_df))
    with col2:
        top_scorer = filtered_df.loc[filtered_df['goals_per_game'].idxmax()]
        st.metric("Top Scorer", f"{top_scorer['player_name']}", f"{top_scorer['goals_per_game']:.2f} goals/game")
    with col3:
        top_assist = filtered_df.loc[filtered_df['assists_per_game'].idxmax()]
        st.metric("Top Assist Provider", f"{top_assist['player_name']}", f"{top_assist['assists_per_game']:.2f} assists/game")
    with col4:
        if 'avg_rating' in filtered_df.columns and not filtered_df['avg_rating'].isna().all():
            top_rated = filtered_df.loc[filtered_df['avg_rating'].idxmax()]
            st.metric("Highest Rated", f"{top_rated['player_name']}", f"{top_rated['avg_rating']:.2f} rating")
    
    # Performance charts
    tab1, tab2, tab3 = st.tabs(["⚽ Goal Scorers", "🎯 Assists & Ratings", "🃏 Cards & Discipline"])
    
    with tab1:
        # Top goal scorers
        top_scorers = filtered_df.nlargest(15, 'goals_per_game')
        
        fig = px.bar(
            top_scorers,
            x='goals_per_game',
            y='player_name',
            color='position',
            title="Top 15 Goal Scorers (Goals per Game)",
            orientation='h',
            hover_data=['team_name', 'total_goals', 'appearances']
        )
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
        
        # Goals vs assists scatter
        fig2 = px.scatter(
            filtered_df,
            x='goals_per_game',
            y='assists_per_game',
            color='position',
            size='appearances',
            hover_name='player_name',
            title="Goals vs Assists per Game",
            hover_data=['team_name']
        )
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    with tab2:
        # Top assist providers
        top_assists = filtered_df.nlargest(15, 'assists_per_game')
        
        fig = px.bar(
            top_assists,
            x='assists_per_game',
            y='player_name',
            color='team_name',
            title="Top 15 Assist Providers (Assists per Game)",
            orientation='h',
            hover_data=['position', 'total_assists', 'appearances']
        )
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
        
        # Rating distribution by position
        if 'avg_rating' in filtered_df.columns and not filtered_df['avg_rating'].isna().all():
            fig2 = px.box(
                filtered_df,
                x='position',
                y='avg_rating',
                title="Average Rating Distribution by Position",
                color='position'
            )
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    with tab3:
        # Cards analysis
        cards_df = filtered_df[filtered_df['cards_per_game'] > 0].copy()
        
        if not cards_df.empty:
            top_cards = cards_df.nlargest(15, 'cards_per_game')
            
            fig = px.bar(
                top_cards,
                x='cards_per_game',
                y='player_name',
                color='position',
                title="Most Disciplined Players (Cards per Game)",
                orientation='h',
                hover_data=['team_name', 'yellow_cards', 'red_cards']
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
            
            # Cards by position
            fig2 = px.box(
                filtered_df,
                x='position',
                y='cards_per_game',
                title="Card Rate Distribution by Position",
                color='position'
            )
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    # Detailed player statistics table
    st.subheader("📋 Detailed Player Statistics")
    
    # Format the dataframe for display
    display_df = filtered_df.copy()
    numeric_columns = ['goals_per_game', 'assists_per_game', 'cards_per_game', 'avg_rating']
    for col in numeric_columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].round(2)
    
    # Rename columns for display
    display_df = display_df.rename(columns={
        'player_name': 'Player',
        'position': 'Position',
        'team_name': 'Team',
        'league': 'League',
        'appearances': 'Apps',
        'total_goals': 'Goals',
        'total_assists': 'Assists',
        'goals_per_game': 'Goals/Game',
        'assists_per_game': 'Assists/Game',
        'yellow_cards': 'Yellow',
        'red_cards': 'Red',
        'cards_per_game': 'Cards/Game',
        'avg_rating': 'Rating',
        'avg_minutes': 'Avg Min'
    })
    
    # Select and reorder columns for display
    display_columns = ['Player', 'Position', 'Team', 'League', 'Apps', 'Goals', 'Assists', 
                      'Goals/Game', 'Assists/Game', 'Yellow', 'Red', 'Cards/Game']
    
    if 'Rating' in display_df.columns:
        display_columns.append('Rating')
    
    # Filter columns that actually exist
    display_columns = [col for col in display_columns if col in display_df.columns]
    
    st.dataframe(
        display_df[display_columns].sort_values('Goals/Game', ascending=False),
        use_container_width=True
    )

def display_feature_insights(model_metadata, trained_models):
    """Display feature importance and insights"""
    st.header("🔍 Feature Insights")
    
    # Get feature importance from tree-based models
    if 'xgboost' in trained_models:
        xgb_results = trained_models['xgboost']['results']
        xgb_importance = xgb_results.get('feature_importance')
        if xgb_importance is not None:
            st.subheader("🌳 XGBoost Feature Importance")
            
            top_features = xgb_importance.head(15)
            
            fig = px.bar(
                top_features,
                x='importance',
                y='feature',
                orientation='h',
                title="Top 15 Most Important Features",
                color='importance',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
            
            # Feature categories analysis
            st.subheader("📊 Feature Categories")
            
            categories = {
                'Form Features': len([f for f in model_metadata['feature_names'] if 'form_' in f]),
                'Statistical Features': len([f for f in model_metadata['feature_names'] if 'stat_' in f]),
                'Head-to-Head Features': len([f for f in model_metadata['feature_names'] if 'h2h_' in f]),
                'Context Features': len([f for f in model_metadata['feature_names'] if any(x in f for x in ['league_', 'season_', 'day_', 'month'])])
            }
            
            cat_df = pd.DataFrame(list(categories.items()), columns=['Category', 'Count'])
            
            fig = px.pie(
                cat_df,
                values='Count',
                names='Category',
                title="Feature Distribution by Category"
            )
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
        else:
            st.warning("XGBoost feature importance not available")
    else:
        st.warning("XGBoost model not available for feature analysis")
    
    # Additional feature insights
    st.subheader("📋 Feature Summary")
    st.write(f"**Total Features:** {len(model_metadata['feature_names'])}")
    
    feature_categories = {
        'Team Form': len([f for f in model_metadata['feature_names'] if 'form_' in f]),
        'Team Statistics': len([f for f in model_metadata['feature_names'] if 'stat_' in f]),
        'Head-to-Head': len([f for f in model_metadata['feature_names'] if 'h2h_' in f]),
        'Context': len([f for f in model_metadata['feature_names'] if any(x in f for x in ['league_', 'season_', 'day_', 'month', 'weekend', 'progress'])])
    }
    
    for category, count in feature_categories.items():
        st.write(f"**{category}:** {count} features")

    """Display feature importance and insights"""
    st.header("🔍 Feature Insights")
    
    # Get feature importance from tree-based models
    if 'xgboost' in trained_models:
        xgb_importance = trained_models['xgboost']['results'].get('feature_importance')
        if xgb_importance is not None:
            st.subheader("🌳 XGBoost Feature Importance")
            
            top_features = xgb_importance.head(15)
            
            fig = px.bar(
                top_features,
                x='importance',
                y='feature',
                orientation='h',
                title="Top 15 Most Important Features",
                color='importance',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
            
            # Feature categories analysis
            st.subheader("📊 Feature Categories")
            
            categories = {
                'Form Features': len([f for f in model_metadata['feature_names'] if 'form_' in f]),
                'Statistical Features': len([f for f in model_metadata['feature_names'] if 'stat_' in f]),
                'Head-to-Head Features': len([f for f in model_metadata['feature_names'] if 'h2h_' in f]),
                'Context Features': len([f for f in model_metadata['feature_names'] if any(x in f for x in ['league_', 'season_', 'day_', 'month'])])
            }
            
            cat_df = pd.DataFrame(list(categories.items()), columns=['Category', 'Count'])
            
            fig = px.pie(
                cat_df,
                values='Count',
                names='Category',
                title="Feature Distribution by Category"
            )
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown('<h1 class="main-header">⚽ Football Betting AI Predictor</h1>', unsafe_allow_html=True)
    st.markdown("### Powered by Machine Learning & Advanced Analytics")
    
    # Load data
    trained_models, model_metadata, streamlit_summary, ml_data = load_models_and_data()
    
    if trained_models is None:
        st.stop()
    
    teams_df = load_team_data()
    
    # Sidebar navigation
    st.sidebar.title("🎛️ Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["🎯 Live Predictor", "🤖 Model Performance", "💰 Betting Analysis", 
         "🔍 Feature Insights", "📊 Dashboard", "⚽ Team Statistics", "👤 Player Statistics"]
    )
    
    # Display key metrics in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📈 Key Performance Metrics")
    st.sidebar.metric("Best Model", streamlit_summary['best_model'])
    st.sidebar.metric("Accuracy", f"{streamlit_summary['best_accuracy']:.3f}")
    st.sidebar.metric("Betting ROI", f"{streamlit_summary['betting_roi']:.1f}%")
    st.sidebar.metric("Win Rate", f"{streamlit_summary['betting_win_rate']:.1%}")
    
    # Page routing
    if page == "🎯 Live Predictor":
        st.header("🎯 Live Match Predictor")
        st.write("Enter match details to get AI-powered predictions and betting recommendations.")
        
        match_input = create_prediction_input_form(teams_df)
        
        if st.button("🔮 Generate Prediction", type="primary"):
            if match_input['home_team'] != match_input['away_team']:
                with st.spinner("Analyzing match data and generating prediction..."):
                    predicted_outcome, prob_dict, confidence = make_prediction(
                        match_input, trained_models, model_metadata
                    )
                    display_prediction_results(predicted_outcome, prob_dict, confidence, match_input)
            else:
                st.error("Please select different teams for home and away.")
    
    elif page == "🤖 Model Performance":
        display_model_performance(model_metadata)
    
    elif page == "💰 Betting Analysis":
        display_betting_analysis(model_metadata)
    
    elif page == "🔍 Feature Insights":
        display_feature_insights(model_metadata, trained_models)
    
    elif page == "⚽ Team Statistics":
        display_team_statistics()
    
    elif page == "👤 Player Statistics":
        display_player_statistics()
    
    elif page == "📊 Dashboard":
        st.header("📊 Executive Dashboard")
        
        # Overview metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="🤖 Best Model",
                value=streamlit_summary['best_model'],
                help="Highest performing model based on test accuracy"
            )
        
        with col2:
            st.metric(
                label="📊 Model Accuracy",
                value=f"{streamlit_summary['best_accuracy']:.3f}",
                delta=f"+{streamlit_summary['best_accuracy'] - model_metadata['baseline_accuracy']:.3f}",
                help="Test set accuracy vs baseline"
            )
        
        with col3:
            st.metric(
                label="💰 Betting ROI",
                value=f"{streamlit_summary['betting_roi']:.1f}%",
                help="Return on investment from betting simulation"
            )
        
        with col4:
            st.metric(
                label="🎯 Total Features",
                value=streamlit_summary['total_features'],
                help="Number of engineered features used for prediction"
            )
        
        # Quick insights
        st.subheader("🎉 Key Achievements")
        
        achievements = [
            f"✅ Achieved {streamlit_summary['best_accuracy']:.1%} prediction accuracy",
            f"✅ Generated {streamlit_summary['betting_roi']:.1f}% ROI in betting simulation",
            f"✅ Processed {model_metadata['training_samples']:,} training samples",
            f"✅ Engineered {streamlit_summary['total_features']} predictive features",
            f"✅ Trained and compared 4 different ML algorithms"
        ]
        
        for achievement in achievements:
            st.write(achievement)
        
        # Model comparison mini-chart
        st.subheader("🏆 Model Performance Comparison")
        models_comparison = model_metadata['models_comparison']
        fig = px.bar(
            models_comparison.head(4),
            x='Model',
            y='Test_Accuracy',
            color='Test_Accuracy',
            color_continuous_scale='RdYlGn',
            title="Model Accuracy Comparison"
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True, key=f"chart_{hash(str(fig))}")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666; padding: 20px;'>
            ⚽ Football Betting AI Predictor | Built with Streamlit & Scikit-learn<br>
            🤖 XGBoost • 🎯 SVM • 🌲 Random Forest • 🧠 Neural Networks
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()