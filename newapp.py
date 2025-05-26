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
    """Load all trained models and data"""
    try:
        # Load trained models
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
    
    except FileNotFoundError as e:
        st.error(f"Required file not found: {e}")
        st.error("Please ensure you've run the model training steps first.")
        return None, None, None, None

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
    """Make prediction using the best model"""
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
    st.plotly_chart(fig, use_container_width=True)
    
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
    st.plotly_chart(fig, use_container_width=True)
    
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
        st.plotly_chart(fig, use_container_width=True)
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
        st.plotly_chart(fig, use_container_width=True)

def display_feature_insights(model_metadata, trained_models):
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
            st.plotly_chart(fig, use_container_width=True)
            
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
            st.plotly_chart(fig, use_container_width=True)

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
        ["🎯 Live Predictor", "🤖 Model Performance", "💰 Betting Analysis", "🔍 Feature Insights", "📊 Dashboard"]
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
        st.plotly_chart(fig, use_container_width=True)
    
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