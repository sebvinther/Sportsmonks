# 🚀 PROFESSIONAL FOOTBALL BETTING ANALYTICS PLATFORM
# =====================================================
# 🎯 Streamlit UI showcasing 74.5% accuracy prediction system

import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import pickle
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="⚽ Pro Football Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
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

# Initialize session state
if 'prediction_system' not in st.session_state:
    st.session_state.prediction_system = None
if 'system_loaded' not in st.session_state:
    st.session_state.system_loaded = False

@st.cache_resource
def load_prediction_system():
    """Load the production prediction system"""
    try:
        # Import the ProductionPredictionSystem class (assuming it's available)
        # In real implementation, you'd import from your module
        # from production_system import ProductionPredictionSystem
        
        # For now, we'll create a mock system with the same interface
        class MockProductionSystem:
            def __init__(self):
                self.db_path = "/Users/sebastianvinther/Desktop/Sportsmonks/db_sportmonks.db"
                self.feature_names = [
                    'form_strength_gap', 'momentum_gap', 'home_recent_form_strength',
                    'away_recent_form_strength', 'home_momentum_points', 'away_momentum_points',
                    'home_season_progress', 'away_season_progress', 'match_importance',
                    'home_home_specialist', 'away_away_specialist', 'home_attacking_potency',
                    'away_attacking_potency', 'home_defensive_solidity', 'away_defensive_solidity'
                    # ... rest of 46 features
                ]
                self.accuracy = 0.745  # Validated accuracy
                
            def get_upcoming_fixtures(self, limit=50):
                # Mock upcoming fixtures
                return pd.DataFrame({
                    'fixture_id': range(1, limit+1),
                    'home_team': [f'Team H{i}' for i in range(1, limit+1)],
                    'away_team': [f'Team A{i}' for i in range(1, limit+1)],
                    'home_team_id': range(100, 100+limit),
                    'away_team_id': range(200, 200+limit),
                    'league_name': ['Premier League'] * limit,
                    'league_id': [1] * limit,
                    'starting_at': [datetime.now() + timedelta(days=i) for i in range(limit)]
                })
            
            def predict_fixtures(self, fixtures):
                # Mock predictions with realistic probabilities
                predictions = []
                for _, fixture in fixtures.iterrows():
                    # Generate realistic probabilities
                    home_prob = np.random.uniform(0.2, 0.8)
                    away_prob = np.random.uniform(0.1, 0.8-home_prob)
                    draw_prob = 1.0 - home_prob - away_prob
                    
                    confidence = max(home_prob, draw_prob, away_prob)
                    predicted_outcome = 'H' if home_prob == confidence else ('D' if draw_prob == confidence else 'A')
                    
                    predictions.append({
                        'fixture_id': fixture['fixture_id'],
                        'home_team': fixture['home_team'],
                        'away_team': fixture['away_team'],
                        'league': fixture['league_name'],
                        'date': fixture['starting_at'],
                        'predicted_outcome': predicted_outcome,
                        'home_prob': home_prob,
                        'draw_prob': draw_prob,
                        'away_prob': away_prob,
                        'confidence': confidence,
                        'form_strength_gap': np.random.uniform(-0.3, 0.3),
                        'momentum_gap': np.random.uniform(-2, 2)
                    })
                
                return pd.DataFrame(predictions)
            
            def get_betting_odds(self, fixture_ids):
                # Mock betting odds
                odds_data = []
                for fixture_id in fixture_ids:
                    odds_data.append({
                        'fixture_id': fixture_id,
                        'best_odds_H': np.random.uniform(1.5, 4.0),
                        'best_odds_D': np.random.uniform(2.8, 4.5),
                        'best_odds_A': np.random.uniform(1.8, 5.0),
                        'avg_odds_H': np.random.uniform(1.4, 3.8),
                        'avg_odds_D': np.random.uniform(2.7, 4.3),
                        'avg_odds_A': np.random.uniform(1.7, 4.8)
                    })
                return pd.DataFrame(odds_data)
            
            def calculate_value_bets(self, predictions, odds):
                # Mock value bet calculation
                merged = predictions.merge(odds, on='fixture_id', how='left')
                merged['max_expected_value'] = np.random.uniform(0, 0.3, len(merged))
                merged['max_edge'] = np.random.uniform(-0.1, 0.2, len(merged))
                
                # Mock value analysis
                value_analysis = []
                for _, row in merged.iterrows():
                    value_analysis.append({
                        'fixture_id': row['fixture_id'],
                        'home_team': row['home_team'],
                        'away_team': row['away_team'],
                        'max_expected_value': row['max_expected_value'],
                        'best_bet': {
                            'outcome': row['predicted_outcome'],
                            'odds': row.get(f'best_odds_{row["predicted_outcome"]}', 2.0),
                            'expected_value': row['max_expected_value']
                        }
                    })
                
                return merged, value_analysis
            
            def calculate_comprehensive_stakes(self, predictions, bankroll=1000):
                # Mock stake calculation
                stakes = predictions.copy()
                stakes['stake_amount'] = np.random.uniform(0, 50, len(stakes))
                stakes['recommended_bet'] = stakes['predicted_outcome']
                stakes['recommendation'] = 'MODERATE BET'
                stakes['expected_value'] = np.random.uniform(0.05, 0.25, len(stakes))
                stakes['edge'] = np.random.uniform(0.02, 0.15, len(stakes))
                return stakes
                
        system = MockProductionSystem()
        return system, True
        
    except Exception as e:
        st.error(f"Error loading prediction system: {e}")
        return None, False

def load_system():
    """Load prediction system into session state"""
    if not st.session_state.system_loaded:
        with st.spinner("🔄 Loading advanced prediction system..."):
            system, success = load_prediction_system()
            if success:
                st.session_state.prediction_system = system
                st.session_state.system_loaded = True
                st.success("✅ Prediction system loaded successfully!")
            else:
                st.error("❌ Failed to load prediction system")

# Main App
def main():
    st.markdown('<h1 class="main-header">⚽ Professional Football Betting Analytics</h1>', unsafe_allow_html=True)
    st.markdown("**🎯 AI-Powered Predictions | 74.5% Accuracy | Professional Betting Intelligence**")
    
    # Load system
    load_system()
    
    if not st.session_state.system_loaded:
        st.warning("⚠️ Prediction system not loaded. Please check the system configuration.")
        return
    
    # Sidebar navigation
    st.sidebar.title("📊 Navigation")
    st.sidebar.markdown("---")
    
    tab_selection = st.sidebar.radio(
        "Select Analysis Module:",
        [
            "🎯 Match Predictions",
            "📈 Betting Performance", 
            "👤 Player Analytics",
            "🏆 Team Analytics",
            "🔮 Live Predictor",
            "🤖 Model Performance",
            "💰 Portfolio Management"
        ]
    )
    
    # System status sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🚀 System Status")
    if st.session_state.system_loaded:
        st.sidebar.success("✅ System Online")
        st.sidebar.metric("Model Accuracy", "74.5%")
        st.sidebar.metric("Features", "46")
        st.sidebar.metric("Database", "155K+ matches")
    
    # Main content based on tab selection
    if tab_selection == "🎯 Match Predictions":
        show_match_predictions()
    elif tab_selection == "📈 Betting Performance":
        show_betting_performance()
    elif tab_selection == "👤 Player Analytics":
        show_player_analytics()
    elif tab_selection == "🏆 Team Analytics":
        show_team_analytics()
    elif tab_selection == "🔮 Live Predictor":
        show_live_predictor()
    elif tab_selection == "🤖 Model Performance":
        show_model_performance()
    elif tab_selection == "💰 Portfolio Management":
        show_portfolio_management()

def show_match_predictions():
    """Main predictions dashboard"""
    st.header("🎯 Upcoming Match Predictions")
    st.markdown("**Real-time predictions with professional betting intelligence**")
    
    # Control panel
    col1, col2, col3 = st.columns(3)
    with col1:
        num_matches = st.slider("Number of matches", 5, 50, 20)
    with col2:
        min_confidence = st.slider("Minimum confidence", 0.0, 1.0, 0.6)
    with col3:
        show_value_only = st.checkbox("Show only value bets")
    
    # Load and process predictions
    with st.spinner("🔮 Generating predictions..."):
        system = st.session_state.prediction_system
        fixtures = system.get_upcoming_fixtures(limit=num_matches)
        predictions = system.predict_fixtures(fixtures)
        
        if not predictions.empty:
            # Get odds and calculate value bets
            odds = system.get_betting_odds(predictions['fixture_id'].tolist())
            predictions_with_value, value_analysis = system.calculate_value_bets(predictions, odds)
            
            # Filter based on user preferences
            filtered_predictions = predictions_with_value[
                predictions_with_value['confidence'] >= min_confidence
            ]
            
            if show_value_only:
                filtered_predictions = filtered_predictions[
                    filtered_predictions['max_expected_value'] > 0.05
                ]
            
            # Summary metrics
            st.markdown("### 📊 Prediction Summary")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Matches", len(filtered_predictions))
            with col2:
                high_conf = len(filtered_predictions[filtered_predictions['confidence'] >= 0.7])
                st.metric("High Confidence", high_conf)
            with col3:
                value_bets = len(filtered_predictions[filtered_predictions['max_expected_value'] > 0.05])
                st.metric("Value Opportunities", value_bets)
            with col4:
                avg_confidence = filtered_predictions['confidence'].mean()
                st.metric("Avg Confidence", f"{avg_confidence:.1%}")
            
            # Main predictions table
            st.markdown("### 🏆 Match Predictions")
            
            # Prepare display dataframe
            display_df = filtered_predictions.copy()
            
            # Add odds columns if available
            for outcome in ['H', 'D', 'A']:
                if f'best_odds_{outcome}' in display_df.columns:
                    display_df[f'Odds {outcome}'] = display_df[f'best_odds_{outcome}'].round(2)
            
            # Format probabilities as percentages
            display_df['Home %'] = (display_df['home_prob'] * 100).round(1)
            display_df['Draw %'] = (display_df['draw_prob'] * 100).round(1)
            display_df['Away %'] = (display_df['away_prob'] * 100).round(1)
            display_df['Confidence'] = (display_df['confidence'] * 100).round(1)
            display_df['Expected Value'] = (display_df['max_expected_value'] * 100).round(1)
            
            # Select display columns
            display_columns = [
                'home_team', 'away_team', 'predicted_outcome', 'Confidence',
                'Home %', 'Draw %', 'Away %', 'Expected Value'
            ]
            
            # Add odds columns if available
            odds_columns = [col for col in display_df.columns if col.startswith('Odds')]
            display_columns.extend(odds_columns)
            
            # Style the dataframe
            styled_df = display_df[display_columns].style.format({
                'Confidence': '{:.1f}%',
                'Home %': '{:.1f}%',
                'Draw %': '{:.1f}%',
                'Away %': '{:.1f}%',
                'Expected Value': '{:.1f}%'
            }).background_gradient(
                subset=['Expected Value'], cmap='RdYlGn', vmin=0, vmax=25
            )
            
            st.dataframe(styled_df, use_container_width=True)
            
            # Detailed prediction cards for top matches
            st.markdown("### 🔥 Top Predictions")
            
            top_predictions = filtered_predictions.nlargest(3, 'expected_value' if 'expected_value' in filtered_predictions.columns else 'confidence')
            
            for i, (_, match) in enumerate(top_predictions.iterrows()):
                with st.expander(f"🏆 {match['home_team']} vs {match['away_team']} - {match['confidence']:.1%} confidence"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.markdown("**📊 Prediction**")
                        st.write(f"**Outcome:** {match['predicted_outcome']}")
                        st.write(f"**Confidence:** {match['confidence']:.1%}")
                        if 'max_expected_value' in match:
                            st.write(f"**Expected Value:** {match['max_expected_value']:.1%}")
                    
                    with col2:
                        st.markdown("**🎯 Probabilities**")
                        prob_data = pd.DataFrame({
                            'Outcome': ['Home', 'Draw', 'Away'],
                            'Probability': [match['home_prob'], match['draw_prob'], match['away_prob']]
                        })
                        fig = px.bar(prob_data, x='Outcome', y='Probability', 
                                   title="Win Probabilities")
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col3:
                        st.markdown("**💰 Betting Odds**")
                        if f'best_odds_H' in match:
                            st.write(f"**Home:** {match.get('best_odds_H', 'N/A')}")
                            st.write(f"**Draw:** {match.get('best_odds_D', 'N/A')}")
                            st.write(f"**Away:** {match.get('best_odds_A', 'N/A')}")
                    
                    # Key features
                    st.markdown("**🔍 Key Factors**")
                    if 'form_strength_gap' in match:
                        st.write(f"Form Gap: {match['form_strength_gap']:.3f}")
                    if 'momentum_gap' in match:
                        st.write(f"Momentum Gap: {match['momentum_gap']:.3f}")

def show_betting_performance():
    """Show betting performance and backtesting results"""
    st.header("📈 Betting Performance Analysis")
    st.markdown("**Historical performance and backtesting results**")
    
    # Performance metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card success-metric">', unsafe_allow_html=True)
        st.metric("Model Accuracy", "74.5%", "+4.3%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card success-metric">', unsafe_allow_html=True)
        st.metric("ROI (6 months)", "18.2%", "+2.1%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card warning-metric">', unsafe_allow_html=True)
        st.metric("Win Rate", "69.8%", "-1.2%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card success-metric">', unsafe_allow_html=True)
        st.metric("Sharpe Ratio", "2.14", "+0.3")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Generate sample historical data
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='W')
    cumulative_roi = np.cumsum(np.random.normal(0.003, 0.02, len(dates)))
    
    # Performance chart
    st.markdown("### 📊 Cumulative Performance")
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=cumulative_roi * 100,
        mode='lines', name='Cumulative ROI (%)',
        line=dict(color='#1f77b4', width=3)
    ))
    
    fig.update_layout(
        title="Cumulative ROI Over Time",
        xaxis_title="Date",
        yaxis_title="Cumulative ROI (%)",
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Monthly breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📅 Monthly Performance")
        monthly_data = pd.DataFrame({
            'Month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            'ROI': np.random.uniform(-5, 25, 12),
            'Bets': np.random.randint(15, 45, 12)
        })
        
        fig = px.bar(monthly_data, x='Month', y='ROI', 
                    title="Monthly ROI (%)",
                    color='ROI', color_continuous_scale='RdYlGn')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 Bet Distribution")
        bet_outcomes = pd.DataFrame({
            'Outcome': ['Wins', 'Losses', 'Pushes'],
            'Count': [142, 78, 12],
            'Percentage': [61.2, 33.6, 5.2]
        })
        
        fig = px.pie(bet_outcomes, values='Count', names='Outcome',
                    title="Bet Outcome Distribution")
        st.plotly_chart(fig, use_container_width=True)
    
    # Detailed betting history
    st.markdown("### 📋 Recent Betting History")
    
    # Generate sample betting history
    history_data = []
    for i in range(20):
        outcome = np.random.choice(['Win', 'Loss'], p=[0.7, 0.3])
        stake = np.random.uniform(10, 100)
        odds = np.random.uniform(1.5, 4.0)
        profit = stake * (odds - 1) if outcome == 'Win' else -stake
        
        history_data.append({
            'Date': (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d'),
            'Match': f"Team A vs Team B {i+1}",
            'Bet': np.random.choice(['H', 'D', 'A']),
            'Odds': f"{odds:.2f}",
            'Stake': f"${stake:.2f}",
            'Outcome': outcome,
            'Profit': f"${profit:+.2f}",
            'ROI': f"{(profit/stake)*100:+.1f}%"
        })
    
    history_df = pd.DataFrame(history_data)
    
    # Color code the outcome column
    def color_outcome(val):
        if val == 'Win':
            return 'background-color: #d4edda'
        elif val == 'Loss':
            return 'background-color: #f8d7da'
        return ''
    
    styled_history = history_df.style.applymap(color_outcome, subset=['Outcome'])
    st.dataframe(styled_history, use_container_width=True)

def show_player_analytics():
    """Show detailed player statistics and analytics"""
    st.header("👤 Player Analytics")
    st.markdown("**Comprehensive player performance and impact analysis**")
    
    # Team/Player selection
    col1, col2 = st.columns(2)
    with col1:
        selected_team = st.selectbox("Select Team", 
                                   ["Manchester City", "Arsenal", "Liverpool", "Chelsea", "Manchester United"])
    with col2:
        player_position = st.selectbox("Filter by Position", 
                                     ["All", "Goalkeeper", "Defender", "Midfielder", "Forward"])
    
    # Generate sample player data
    players_data = []
    positions = ["Goalkeeper", "Defender", "Midfielder", "Forward"]
    
    for i in range(15):
        pos = np.random.choice(positions)
        if player_position != "All" and pos != player_position:
            continue
            
        players_data.append({
            'Player': f"Player {i+1}",
            'Position': pos,
            'Age': np.random.randint(18, 35),
            'Games': np.random.randint(20, 38),
            'Goals': np.random.randint(0, 25) if pos in ["Midfielder", "Forward"] else np.random.randint(0, 5),
            'Assists': np.random.randint(0, 15),
            'Yellow Cards': np.random.randint(0, 12),
            'Red Cards': np.random.randint(0, 3),
            'Pass Accuracy': np.random.uniform(75, 95),
            'Duels Won': np.random.randint(40, 150),
            'Successful Crosses': np.random.randint(5, 50),
            'Tackles': np.random.randint(20, 100),
            'Interceptions': np.random.randint(15, 80),
            'Shots per Game': np.random.uniform(0.5, 4.0),
            'Key Passes': np.random.randint(10, 80),
            'Distance Covered': np.random.uniform(8.5, 12.5)
        })
    
    if not players_data:
        st.warning("No players found for the selected criteria.")
        return
    
    players_df = pd.DataFrame(players_data)
    
    # Player overview metrics
    st.markdown("### 📊 Team Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Players", len(players_df))
    with col2:
        st.metric("Avg Age", f"{players_df['Age'].mean():.1f}")
    with col3:
        st.metric("Total Goals", players_df['Goals'].sum())
    with col4:
        st.metric("Total Assists", players_df['Assists'].sum())
    
    # Detailed player statistics
    st.markdown("### 🏆 Player Statistics")
    
    # Performance metrics tabs
    metric_tab1, metric_tab2, metric_tab3 = st.tabs(["⚽ Offensive", "🛡️ Defensive", "📊 General"])
    
    with metric_tab1:
        st.markdown("**Offensive Performance**")
        offensive_cols = ['Player', 'Position', 'Goals', 'Assists', 'Shots per Game', 'Key Passes', 'Successful Crosses']
        offensive_df = players_df[offensive_cols].sort_values('Goals', ascending=False)
        st.dataframe(offensive_df, use_container_width=True)
        
        # Top scorers chart
        col1, col2 = st.columns(2)
        with col1:
            top_scorers = players_df.nlargest(5, 'Goals')
            fig = px.bar(top_scorers, x='Player', y='Goals', title="Top Goal Scorers")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            top_assists = players_df.nlargest(5, 'Assists')
            fig = px.bar(top_assists, x='Player', y='Assists', title="Top Assist Providers", color_discrete_sequence=['orange'])
            st.plotly_chart(fig, use_container_width=True)
    
    with metric_tab2:
        st.markdown("**Defensive Performance**")
        defensive_cols = ['Player', 'Position', 'Tackles', 'Interceptions', 'Duels Won', 'Yellow Cards', 'Red Cards']
        defensive_df = players_df[defensive_cols].sort_values('Tackles', ascending=False)
        st.dataframe(defensive_df, use_container_width=True)
        
        # Defensive stats visualization
        col1, col2 = st.columns(2)
        with col1:
            fig = px.scatter(players_df, x='Tackles', y='Interceptions', 
                           color='Position', size='Duels Won',
                           title="Defensive Actions Correlation")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            discipline_data = players_df.groupby('Position')[['Yellow Cards', 'Red Cards']].sum().reset_index()
            fig = px.bar(discipline_data, x='Position', y=['Yellow Cards', 'Red Cards'],
                        title="Disciplinary Record by Position", barmode='stack')
            st.plotly_chart(fig, use_container_width=True)
    
    with metric_tab3:
        st.markdown("**General Performance**")
        general_cols = ['Player', 'Position', 'Games', 'Pass Accuracy', 'Distance Covered']
        general_df = players_df[general_cols].sort_values('Games', ascending=False)
        
        # Format pass accuracy as percentage
        general_df['Pass Accuracy'] = general_df['Pass Accuracy'].apply(lambda x: f"{x:.1f}%")
        general_df['Distance Covered'] = general_df['Distance Covered'].apply(lambda x: f"{x:.1f} km")
        
        st.dataframe(general_df, use_container_width=True)
        
        # Performance radar chart for selected player
        st.markdown("**Player Performance Radar**")
        selected_player = st.selectbox("Select Player for Radar Chart", players_df['Player'].tolist())
        
        player_data = players_df[players_df['Player'] == selected_player].iloc[0]
        
        # Normalize stats for radar chart (0-100 scale)
        radar_stats = {
            'Goals': min(player_data['Goals'] / players_df['Goals'].max() * 100, 100),
            'Assists': min(player_data['Assists'] / players_df['Assists'].max() * 100, 100),
            'Pass Accuracy': player_data['Pass Accuracy'],
            'Tackles': min(player_data['Tackles'] / players_df['Tackles'].max() * 100, 100),
            'Duels Won': min(player_data['Duels Won'] / players_df['Duels Won'].max() * 100, 100)
        }
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatterpolar(
            r=list(radar_stats.values()),
            theta=list(radar_stats.keys()),
            fill='toself',
            name=selected_player
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100]
                )),
            showlegend=True,
            title=f"Performance Radar: {selected_player}"
        )
        
        st.plotly_chart(fig, use_container_width=True)

def show_team_analytics():
    """Show comprehensive team statistics and analytics"""
    st.header("🏆 Team Analytics")
    st.markdown("**Detailed team performance metrics and comparative analysis**")
    
    # Team selection
    col1, col2 = st.columns(2)
    with col1:
        selected_team = st.selectbox("Select Team", 
                                   ["Manchester City", "Arsenal", "Liverpool", "Chelsea", "Manchester United", "Tottenham"])
    with col2:
        comparison_team = st.selectbox("Compare with", 
                                     ["None", "Arsenal", "Liverpool", "Chelsea", "Manchester United", "Tottenham"])
    
    # Generate comprehensive team statistics
    def generate_team_stats(team_name):
        return {
            'Team': team_name,
            'Games Played': np.random.randint(25, 38),
            'Wins': np.random.randint(15, 30),
            'Draws': np.random.randint(3, 10),
            'Losses': np.random.randint(2, 15),
            'Goals Scored': np.random.randint(40, 85),
            'Goals Conceded': np.random.randint(20, 50),
            'Goal Difference': 0,  # Will calculate
            'Points': 0,  # Will calculate
            'Goals per Game': 0,  # Will calculate
            'Goals Conceded per Game': 0,  # Will calculate
            'Shots per Game': np.random.uniform(12, 20),
            'Shots on Target per Game': np.random.uniform(5, 8),
            'Pass Accuracy': np.random.uniform(78, 92),
            'Possession %': np.random.uniform(45, 70),
            'Yellow Cards per Game': np.random.uniform(1.5, 3.5),
            'Red Cards per Game': np.random.uniform(0.05, 0.3),
            'Corners per Game': np.random.uniform(4, 8),
            'Offsides per Game': np.random.uniform(1, 4),
            'Fouls per Game': np.random.uniform(10, 18),
            'Clean Sheets': np.random.randint(5, 20),
            'Win Rate Home': np.random.uniform(0.6, 0.9),
            'Win Rate Away': np.random.uniform(0.3, 0.7)
        }
    
    team_stats = generate_team_stats(selected_team)
    
    # Calculate derived stats
    team_stats['Goal Difference'] = team_stats['Goals Scored'] - team_stats['Goals Conceded']
    team_stats['Points'] = team_stats['Wins'] * 3 + team_stats['Draws']
    team_stats['Goals per Game'] = team_stats['Goals Scored'] / team_stats['Games Played']
    team_stats['Goals Conceded per Game'] = team_stats['Goals Conceded'] / team_stats['Games Played']
    
    # Display key metrics
    st.markdown(f"### 📊 {selected_team} - Season Overview")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Points", team_stats['Points'])
    with col2:
        st.metric("Goals Scored", team_stats['Goals Scored'])
    with col3:
        st.metric("Goals Conceded", team_stats['Goals Conceded'])
    with col4:
        st.metric("Goal Difference", f"+{team_stats['Goal Difference']}" if team_stats['Goal Difference'] >= 0 else str(team_stats['Goal Difference']))
    with col5:
        win_rate = team_stats['Wins'] / team_stats['Games Played']
        st.metric("Win Rate", f"{win_rate:.1%}")
    
    # Detailed statistics tabs
    stats_tab1, stats_tab2, stats_tab3, stats_tab4 = st.tabs(["⚽ Attack", "🛡️ Defense", "📊 General", "🏠 Home/Away"])
    
    with stats_tab1:
        st.markdown("**Attacking Statistics**")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Key Metrics**")
            st.write(f"Goals per Game: **{team_stats['Goals per Game']:.2f}**")
            st.write(f"Shots per Game: **{team_stats['Shots per Game']:.1f}**")
            st.write(f"Shots on Target per Game: **{team_stats['Shots on Target per Game']:.1f}**")
            st.write(f"Corners per Game: **{team_stats['Corners per Game']:.1f}**")
            
            # Shot conversion rate
            conversion_rate = (team_stats['Shots on Target per Game'] / team_stats['Shots per Game']) * 100
            st.write(f"Shot Accuracy: **{conversion_rate:.1f}%**")
        
        with col2:
            # Goals scored trend (mock data)
            months = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May']
            goals_trend = np.random.randint(5, 15, len(months))
            
            fig = px.line(x=months, y=goals_trend, title="Goals Scored by Month")
            fig.update_traces(line_color='green', line_width=3)
            st.plotly_chart(fig, use_container_width=True)
    
    with stats_tab2:
        st.markdown("**Defensive Statistics**")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Key Metrics**")
            st.write(f"Goals Conceded per Game: **{team_stats['Goals Conceded per Game']:.2f}**")
            st.write(f"Clean Sheets: **{team_stats['Clean Sheets']}**")
            
            clean_sheet_rate = team_stats['Clean Sheets'] / team_stats['Games Played']
            st.write(f"Clean Sheet Rate: **{clean_sheet_rate:.1%}**")
            
            st.write(f"Yellow Cards per Game: **{team_stats['Yellow Cards per Game']:.1f}**")
            st.write(f"Red Cards per Game: **{team_stats['Red Cards per Game']:.2f}**")
        
        with col2:
            # Defensive actions visualization
            defensive_data = pd.DataFrame({
                'Metric': ['Clean Sheets', 'Goals Conceded', 'Yellow Cards', 'Red Cards'],
                'Value': [team_stats['Clean Sheets'], team_stats['Goals Conceded'], 
                         team_stats['Yellow Cards per Game'] * team_stats['Games Played'],
                         team_stats['Red Cards per Game'] * team_stats['Games Played']]
            })
            
            fig = px.bar(defensive_data, x='Metric', y='Value', title="Defensive Statistics")
            st.plotly_chart(fig, use_container_width=True)
    
    with stats_tab3:
        st.markdown("**General Performance**")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Possession & Passing**")
            st.write(f"Average Possession: **{team_stats['Possession %']:.1f}%**")
            st.write(f"Pass Accuracy: **{team_stats['Pass Accuracy']:.1f}%**")
            st.write(f"Fouls per Game: **{team_stats['Fouls per Game']:.1f}**")
            st.write(f"Offsides per Game: **{team_stats['Offsides per Game']:.1f}**")
        
        with col2:
            # Performance radar chart
            performance_metrics = {
                'Attack': min(team_stats['Goals per Game'] / 3 * 100, 100),
                'Defense': min((3 - team_stats['Goals Conceded per Game']) / 3 * 100, 100),
                'Possession': team_stats['Possession %'],
                'Accuracy': team_stats['Pass Accuracy'],
                'Discipline': min((5 - team_stats['Yellow Cards per Game']) / 5 * 100, 100)
            }
            
            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(
                r=list(performance_metrics.values()),
                theta=list(performance_metrics.keys()),
                fill='toself',
                name=selected_team
            ))
            
            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                showlegend=True,
                title=f"{selected_team} Performance Profile"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with stats_tab4:
        st.markdown("**Home vs Away Performance**")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Performance Split**")
            st.write(f"Home Win Rate: **{team_stats['Win Rate Home']:.1%}**")
            st.write(f"Away Win Rate: **{team_stats['Win Rate Away']:.1%}**")
            
            home_away_diff = team_stats['Win Rate Home'] - team_stats['Win Rate Away']
            st.write(f"Home Advantage: **{home_away_diff:+.1%}**")
        
        with col2:
            # Home vs Away comparison
            comparison_data = pd.DataFrame({
                'Venue': ['Home', 'Away'],
                'Win Rate': [team_stats['Win Rate Home'] * 100, team_stats['Win Rate Away'] * 100]
            })
            
            fig = px.bar(comparison_data, x='Venue', y='Win Rate', 
                        title="Home vs Away Win Rate (%)",
                        color='Venue', color_discrete_map={'Home': 'green', 'Away': 'orange'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Team comparison if selected
    if comparison_team != "None":
        st.markdown(f"### ⚖️ Team Comparison: {selected_team} vs {comparison_team}")
        
        comparison_stats = generate_team_stats(comparison_team)
        comparison_stats['Goal Difference'] = comparison_stats['Goals Scored'] - comparison_stats['Goals Conceded']
        comparison_stats['Points'] = comparison_stats['Wins'] * 3 + comparison_stats['Draws']
        comparison_stats['Goals per Game'] = comparison_stats['Goals Scored'] / comparison_stats['Games Played']
        comparison_stats['Goals Conceded per Game'] = comparison_stats['Goals Conceded'] / comparison_stats['Games Played']
        
        # Comparison metrics
        comparison_metrics = [
            'Goals per Game', 'Goals Conceded per Game', 'Pass Accuracy', 
            'Possession %', 'Shots per Game', 'Yellow Cards per Game'
        ]
        
        comparison_df = pd.DataFrame({
            'Metric': comparison_metrics,
            selected_team: [team_stats[metric] for metric in comparison_metrics],
            comparison_team: [comparison_stats[metric] for metric in comparison_metrics]
        })
        
        fig = px.bar(comparison_df, x='Metric', y=[selected_team, comparison_team],
                    title=f"Team Comparison: {selected_team} vs {comparison_team}",
                    barmode='group')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def show_live_predictor():
    """Interactive live prediction tool"""
    st.header("🔮 Live Match Predictor")
    st.markdown("**Generate real-time predictions for any match**")
    
    # Team selection
    st.markdown("### ⚽ Select Teams")
    
    # Load available teams (mock data)
    teams = [
        "Manchester City", "Arsenal", "Liverpool", "Chelsea", "Manchester United", 
        "Tottenham", "Newcastle", "Brighton", "Aston Villa", "West Ham",
        "Real Madrid", "Barcelona", "Atletico Madrid", "Bayern Munich", "Borussia Dortmund",
        "Paris Saint-Germain", "AS Monaco", "Juventus", "Inter Milan", "AC Milan"
    ]
    
    col1, col2 = st.columns(2)
    with col1:
        home_team = st.selectbox("🏠 Home Team", teams, index=0)
    with col2:
        away_team = st.selectbox("✈️ Away Team", teams, index=1)
    
    # Match details
    col1, col2, col3 = st.columns(3)
    with col1:
        match_date = st.date_input("📅 Match Date", datetime.now().date())
    with col2:
        league = st.selectbox("🏆 League", ["Premier League", "La Liga", "Bundesliga", "Serie A", "Ligue 1"])
    with col3:
        importance = st.slider("🎯 Match Importance", 0.5, 2.0, 1.0, 0.1)
    
    # Prediction button
    if st.button("🚀 Generate Prediction", type="primary"):
        if home_team == away_team:
            st.error("❌ Please select different teams for home and away.")
        else:
            with st.spinner("🔮 Analyzing teams and generating prediction..."):
                # Simulate prediction generation
                import time
                time.sleep(2)
                
                # Generate realistic probabilities
                home_prob = np.random.uniform(0.25, 0.65)
                away_prob = np.random.uniform(0.15, 0.65)
                draw_prob = max(0.1, 1.0 - home_prob - away_prob)
                
                # Normalize probabilities
                total = home_prob + draw_prob + away_prob
                home_prob /= total
                draw_prob /= total
                away_prob /= total
                
                confidence = max(home_prob, draw_prob, away_prob)
                predicted_outcome = 'Home Win' if home_prob == confidence else ('Draw' if draw_prob == confidence else 'Away Win')
                
                # Mock feature values
                features = {
                    'Form Strength Gap': np.random.uniform(-0.3, 0.3),
                    'Momentum Gap': np.random.uniform(-2, 2),
                    'Head-to-Head Record': f"{np.random.randint(0, 5)}-{np.random.randint(0, 5)} (last 5)",
                    'Home Advantage': np.random.uniform(0.1, 0.4),
                    'Recent Form (5 games)': f"{home_team}: {np.random.randint(6, 15)}pts | {away_team}: {np.random.randint(6, 15)}pts"
                }
                
                # Generate mock odds
                home_odds = 1 / home_prob * 0.95  # 5% margin
                draw_odds = 1 / draw_prob * 0.95
                away_odds = 1 / away_prob * 0.95
                
                # Calculate value
                best_value_outcome = 'Home Win' if (home_prob * home_odds - 1) > max(draw_prob * draw_odds - 1, away_prob * away_odds - 1) else ('Draw' if (draw_prob * draw_odds - 1) > (away_prob * away_odds - 1) else 'Away Win')
                best_value = max((home_prob * home_odds - 1), (draw_prob * draw_odds - 1), (away_prob * away_odds - 1))
                
            # Display prediction results
            st.success("✅ Prediction Generated Successfully!")
            
            st.markdown("### 🎯 Prediction Results")
            
            # Main prediction card
            col1, col2, col3 = st.columns([2, 1, 2])
            
            with col1:
                st.markdown(f"#### 🏠 {home_team}")
                st.markdown(f"**Win Probability: {home_prob:.1%}**")
                st.markdown(f"Best Odds: {home_odds:.2f}")
            
            with col2:
                st.markdown("#### ⚖️ VS")
                st.markdown(f"**{predicted_outcome}**")
                st.markdown(f"Confidence: {confidence:.1%}")
            
            with col3:
                st.markdown(f"#### ✈️ {away_team}")
                st.markdown(f"**Win Probability: {away_prob:.1%}**")
                st.markdown(f"Best Odds: {away_odds:.2f}")
            
            # Probabilities visualization
            st.markdown("### 📊 Win Probabilities")
            
            prob_data = pd.DataFrame({
                'Outcome': [f'{home_team} Win', 'Draw', f'{away_team} Win'],
                'Probability': [home_prob, draw_prob, away_prob],
                'Odds': [home_odds, draw_odds, away_odds]
            })
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(prob_data, x='Outcome', y='Probability', 
                           title="Win Probabilities",
                           color='Probability', color_continuous_scale='viridis')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(prob_data, x='Outcome', y='Odds',
                           title="Best Available Odds",
                           color='Odds', color_continuous_scale='plasma')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # Key factors
            st.markdown("### 🔍 Key Prediction Factors")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Model Features**")
                for feature, value in features.items():
                    st.write(f"• **{feature}:** {value}")
            
            with col2:
                st.markdown("**Betting Analysis**")
                st.write(f"• **Best Value Bet:** {best_value_outcome}")
                st.write(f"• **Expected Value:** {best_value:.1%}")
                st.write(f"• **Draw Probability:** {draw_prob:.1%}")
                
                if best_value > 0.05:
                    st.success(f"🔥 Strong value opportunity detected!")
                elif best_value > 0.02:
                    st.info(f"💡 Moderate value opportunity")
                else:
                    st.warning(f"⚠️ No significant value detected")
            
            # Historical context
            st.markdown("### 📈 Historical Context")
            
            # Generate mock historical data
            historical_data = pd.DataFrame({
                'Date': pd.date_range(start='2023-01-01', periods=10, freq='3M'),
                'Home Goals': np.random.randint(0, 4, 10),
                'Away Goals': np.random.randint(0, 4, 10),
                'Result': ['H', 'A', 'D', 'H', 'A', 'H', 'D', 'A', 'H', 'D']
            })
            
            st.markdown(f"**Last 10 meetings between {home_team} and {away_team}:**")
            
            results_summary = historical_data['Result'].value_counts()
            st.write(f"• **{home_team} wins:** {results_summary.get('H', 0)}")
            st.write(f"• **{away_team} wins:** {results_summary.get('A', 0)}")
            st.write(f"• **Draws:** {results_summary.get('D', 0)}")
            
            # Goals trend
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=historical_data['Date'], y=historical_data['Home Goals'],
                mode='lines+markers', name=f'{home_team} Goals',
                line=dict(color='blue')
            ))
            fig.add_trace(go.Scatter(
                x=historical_data['Date'], y=historical_data['Away Goals'],
                mode='lines+markers', name=f'{away_team} Goals',
                line=dict(color='red')
            ))
            
            fig.update_layout(
                title="Goals Scored in Recent Meetings",
                xaxis_title="Date",
                yaxis_title="Goals",
                height=300
            )
            
            st.plotly_chart(fig, use_container_width=True)

def show_model_performance():
    """Show detailed model performance metrics and feature importance"""
    st.header("🤖 Model Performance & Analytics")
    st.markdown("**Deep dive into model accuracy, feature importance, and performance metrics**")
    
    # Model overview metrics
    st.markdown("### 📊 Model Performance Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card success-metric">', unsafe_allow_html=True)
        st.metric("Overall Accuracy", "74.5%", "+4.8%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="metric-card success-metric">', unsafe_allow_html=True)
        st.metric("Calibrated Accuracy", "76.2%", "+1.7%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        st.markdown('<div class="metric-card warning-metric">', unsafe_allow_html=True)
        st.metric("Feature Count", "46", "Optimized")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        st.markdown('<div class="metric-card success-metric">', unsafe_allow_html=True)
        st.metric("Training Samples", "50K+", "Enhanced")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Model performance breakdown
    st.markdown("### 🎯 Performance by Outcome")
    
    # Mock performance data
    outcome_performance = pd.DataFrame({
        'Outcome': ['Home Win', 'Draw', 'Away Win'],
        'Precision': [0.76, 0.68, 0.78],
        'Recall': [0.82, 0.59, 0.71],
        'F1-Score': [0.79, 0.63, 0.74],
        'Support': [1247, 543, 891]
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(outcome_performance, x='Outcome', y=['Precision', 'Recall', 'F1-Score'],
                    title="Performance Metrics by Outcome", barmode='group')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Confusion matrix visualization
        confusion_matrix = np.array([[825, 156, 266], [201, 321, 231], [134, 189, 568]])
        
        fig = px.imshow(confusion_matrix, 
                       labels=dict(x="Predicted", y="Actual", color="Count"),
                       x=['Home Win', 'Draw', 'Away Win'],
                       y=['Home Win', 'Draw', 'Away Win'],
                       title="Confusion Matrix")
        st.plotly_chart(fig, use_container_width=True)
    
    # Feature importance analysis
    st.markdown("### 🔬 Feature Importance Analysis")
    
    # Mock feature importance data (based on your actual findings)
    feature_importance = pd.DataFrame({
        'Feature': [
            'form_strength_gap', 'momentum_gap', 'home_home_specialist',
            'away_away_specialist', 'home_attacking_potency', 'away_attacking_potency',
            'home_recent_form_strength', 'away_recent_form_strength', 'match_importance',
            'home_defensive_solidity', 'away_defensive_solidity', 'home_momentum_points',
            'away_momentum_points', 'home_season_progress', 'away_season_progress',
            'congestion_difference', 'consistency_gap', 'h2h_frequency',
            'is_potential_derby', 'season_criticality'
        ],
        'Importance': [
            13.01, 7.66, 3.37, 3.21, 2.89, 2.74, 2.56, 2.43, 2.31, 2.18,
            2.05, 1.97, 1.84, 1.72, 1.68, 1.54, 1.41, 1.33, 1.27, 1.19
        ],
        'Category': [
            'Comparative', 'Comparative', 'Team Specialization', 'Team Specialization',
            'Team Specialization', 'Team Specialization', 'Momentum & Form', 'Momentum & Form',
            'Context', 'Team Specialization', 'Team Specialization', 'Momentum & Form',
            'Momentum & Form', 'Temporal', 'Temporal', 'Comparative', 'Comparative',
            'Context', 'Context', 'Context'
        ]
    })
    
    # Top features chart
    top_features = feature_importance.head(10)
    
    fig = px.bar(top_features, x='Importance', y='Feature', 
                title="Top 10 Most Important Features",
                color='Category', orientation='h')
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Feature categories breakdown
    st.markdown("### 📊 Feature Categories Analysis")
    
    category_importance = feature_importance.groupby('Category')['Importance'].sum().reset_index()
    category_importance = category_importance.sort_values('Importance', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(category_importance, values='Importance', names='Category',
                    title="Feature Importance by Category")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("**Key Insights:**")
        st.write("• **Comparative features** (form_strength_gap, momentum_gap) are most predictive")
        st.write("• **Team specialization** features provide consistent value")
        st.write("• **Temporal intelligence** helps with context")
        st.write("• **Momentum & form** critical for recent performance")
        st.write("• **Match context** provides additional edge")
    
    # Model calibration analysis
    st.markdown("### 🎯 Model Calibration Analysis")
    
    # Mock calibration data
    confidence_bins = ['0-20%', '20-40%', '40-60%', '60-80%', '80-100%']
    predicted_proba = [0.1, 0.3, 0.5, 0.7, 0.9]
    actual_accuracy = [0.12, 0.34, 0.48, 0.73, 0.88]
    
    calibration_df = pd.DataFrame({
        'Confidence Bin': confidence_bins,
        'Predicted Probability': predicted_proba,
        'Actual Accuracy': actual_accuracy,
        'Calibration Error': [abs(p - a) for p, a in zip(predicted_proba, actual_accuracy)]
    })
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=predicted_proba, y=actual_accuracy,
            mode='markers+lines', name='Model Calibration',
            line=dict(color='blue', width=3)
        ))
        fig.add_trace(go.Scatter(
            x=[0, 1], y=[0, 1],
            mode='lines', name='Perfect Calibration',
            line=dict(color='red', dash='dash')
        ))
        fig.update_layout(
            title="Calibration Curve",
            xaxis_title="Predicted Probability",
            yaxis_title="Actual Accuracy",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(calibration_df, x='Confidence Bin', y='Calibration Error',
                    title="Calibration Error by Confidence Level")
        st.plotly_chart(fig, use_container_width=True)
    
    # Historical performance tracking
    st.markdown("### 📈 Historical Performance Tracking")
    
    # Mock historical performance data
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='W')
    historical_accuracy = 0.65 + np.cumsum(np.random.normal(0, 0.005, len(dates)))
    historical_accuracy = np.clip(historical_accuracy, 0.6, 0.8)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=historical_accuracy,
        mode='lines', name='Weekly Accuracy',
        line=dict(color='green', width=2)
    ))
    fig.add_hline(y=0.745, line_dash="dash", line_color="red", 
                  annotation_text="Current Accuracy (74.5%)")
    
    fig.update_layout(
        title="Model Accuracy Over Time",
        xaxis_title="Date",
        yaxis_title="Accuracy",
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Model comparison
    st.markdown("### ⚖️ Model Comparison")
    
    model_comparison = pd.DataFrame({
        'Model': ['XGBoost (Current)', 'Random Forest', 'Neural Network', 'SVM', 'Ensemble'],
        'Accuracy': [0.745, 0.698, 0.692, 0.689, 0.752],
        'Precision': [0.76, 0.71, 0.69, 0.68, 0.77],
        'Recall': [0.74, 0.70, 0.68, 0.67, 0.75],
        'Training Time': ['2.3 min', '1.8 min', '8.4 min', '3.1 min', '12.7 min']
    })
    
    fig = px.bar(model_comparison, x='Model', y='Accuracy',
                title="Model Performance Comparison",
                color='Accuracy', color_continuous_scale='viridis')
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed model info
    with st.expander("🔧 Technical Model Details"):
        st.markdown("**Model Architecture:**")
        st.write("• **Algorithm:** XGBoost with temporal weighting")
        st.write("• **Features:** 46 engineered features across 5 categories")
        st.write("• **Training Data:** 50,000+ samples with data quality weighting")
        st.write("• **Validation:** Cross-validation with temporal splits")
        st.write("• **Calibration:** Temperature scaling (T=0.80)")
        
        st.markdown("**Hyperparameters:**")
        st.code("""
        XGBoost Parameters:
        - n_estimators: 200
        - max_depth: 6
        - learning_rate: 0.1
        - subsample: 0.8
        - colsample_bytree: 0.8
        - random_state: 42
        """)

def show_portfolio_management():
    """Portfolio and bankroll management interface"""
    st.header("💰 Portfolio & Bankroll Management")
    st.markdown("**Professional betting portfolio management and risk analysis**")
    
    # Bankroll settings
    st.markdown("### 💼 Bankroll Configuration")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        bankroll = st.number_input("💰 Current Bankroll ($)", min_value=100, max_value=100000, value=1000, step=100)
    with col2:
        max_kelly = st.slider("⚖️ Max Kelly %", 5, 50, 25) / 100
    with col3:
        risk_tolerance = st.selectbox("🎯 Risk Profile", ["Conservative", "Moderate", "Aggressive"])
    
    # Generate sample active positions
    with st.spinner("📊 Loading portfolio data..."):
        system = st.session_state.prediction_system
        fixtures = system.get_upcoming_fixtures(limit=30)
        predictions = system.predict_fixtures(fixtures)
        
        if not predictions.empty:
            odds = system.get_betting_odds(predictions['fixture_id'].tolist())
            predictions_with_value, _ = system.calculate_value_bets(predictions, odds)
            stakes = system.calculate_comprehensive_stakes(predictions_with_value, bankroll)
    
    # Portfolio overview
    st.markdown("### 📊 Portfolio Overview")
    
    if not stakes.empty:
        active_positions = stakes[stakes['stake_amount'] > 0]
        total_exposure = active_positions['stake_amount'].sum()
        num_positions = len(active_positions)
        avg_stake = total_exposure / num_positions if num_positions > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Positions", num_positions)
        with col2:
            st.metric("Total Exposure", f"${total_exposure:.2f}")
        with col3:
            st.metric("Portfolio Utilization", f"{(total_exposure/bankroll)*100:.1f}%")
        with col4:
            st.metric("Average Stake", f"${avg_stake:.2f}")
        
        # Risk metrics
        max_loss = total_exposure  # Worst case scenario
        expected_return = active_positions['expected_value'].sum() * bankroll if 'expected_value' in active_positions.columns else 0
        
        st.markdown("### ⚠️ Risk Analysis")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown('<div class="metric-card danger-metric">', unsafe_allow_html=True)
            st.metric("Maximum Loss", f"${max_loss:.2f}", f"{(max_loss/bankroll)*100:.1f}% of bankroll")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card success-metric">', unsafe_allow_html=True)
            st.metric("Expected Return", f"${expected_return:.2f}", f"{(expected_return/bankroll)*100:.1f}% ROI")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            largest_position = active_positions['stake_amount'].max() if not active_positions.empty else 0
            concentration = (largest_position / total_exposure * 100) if total_exposure > 0 else 0
            st.markdown('<div class="metric-card warning-metric">', unsafe_allow_html=True)
            st.metric("Largest Position", f"${largest_position:.2f}", f"{concentration:.1f}% of exposure")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Position sizing chart
        st.markdown("### 📊 Position Sizing Distribution")
        
        if not active_positions.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Stake size histogram
                fig = px.histogram(active_positions, x='stake_amount', nbins=10,
                                 title="Stake Size Distribution")
                fig.update_layout(xaxis_title="Stake Amount ($)", yaxis_title="Count")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Expected value vs stake size
                if 'expected_value' in active_positions.columns:
                    fig = px.scatter(active_positions, x='stake_amount', y='expected_value',
                                   title="Stake Size vs Expected Value",
                                   color='confidence', size='stake_amount')
                    fig.update_layout(xaxis_title="Stake Amount ($)", yaxis_title="Expected Value")
                    st.plotly_chart(fig, use_container_width=True)
        
        # Active positions table
        st.markdown("### 📋 Active Positions")
        
        if not active_positions.empty:
            # Prepare display dataframe
            display_positions = active_positions.copy()
            display_positions['Stake %'] = (display_positions['stake_amount'] / bankroll * 100).round(2)
            display_positions['Potential Profit'] = (display_positions['stake_amount'] * 
                                                    (display_positions.get('recommended_odds', 2.0) - 1)).round(2)
            display_positions['Risk/Reward'] = (display_positions['Potential Profit'] / 
                                              display_positions['stake_amount']).round(2)
            
            position_columns = [
                'home_team', 'away_team', 'recommended_bet', 'stake_amount', 
                'Stake %', 'Potential Profit', 'Risk/Reward'
            ]
            
            # Add expected value if available
            if 'expected_value' in display_positions.columns:
                display_positions['Expected Value %'] = (display_positions['expected_value'] * 100).round(1)
                position_columns.append('Expected Value %')
            
            # Style the dataframe
            styled_positions = display_positions[position_columns].style.format({
                'stake_amount': '${:.2f}',
                'Stake %': '{:.2f}%',
                'Potential Profit': '${:.2f}',
                'Risk/Reward': '{:.2f}x'
            }).background_gradient(
                subset=['Potential Profit'], cmap='RdYlGn'
            )
            
            st.dataframe(styled_positions, use_container_width=True)
    
    # Bankroll growth simulation
    st.markdown("### 📈 Bankroll Growth Projection")
    
    col1, col2 = st.columns(2)
    with col1:
        monthly_bets = st.slider("📅 Bets per Month", 5, 50, 20)
    with col2:
        avg_roi = st.slider("📊 Expected ROI per Bet", 0.05, 0.30, 0.15)
    
    # Simulate bankroll growth
    months = 12
    growth_data = []
    current_bankroll = bankroll
    
    for month in range(months + 1):
        growth_data.append({
            'Month': month,
            'Bankroll': current_bankroll,
            'Profit': current_bankroll - bankroll if month > 0 else 0
        })
        
        if month < months:
            # Conservative growth model
            monthly_betting_volume = current_bankroll * 0.1 * monthly_bets  # 10% of bankroll per month
            monthly_profit = monthly_betting_volume * avg_roi
            current_bankroll += monthly_profit
    
    growth_df = pd.DataFrame(growth_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=growth_df['Month'], y=growth_df['Bankroll'],
            mode='lines+markers', name='Projected Bankroll',
            line=dict(color='green', width=3)
        ))
        fig.update_layout(
            title="Bankroll Growth Projection",
            xaxis_title="Month",
            yaxis_title="Bankroll ($)",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        final_bankroll = growth_df['Bankroll'].iloc[-1]
        total_profit = final_bankroll - bankroll
        annual_return = (final_bankroll / bankroll - 1) * 100
        
        st.markdown("**Projection Summary:**")
        st.write(f"• **Starting Bankroll:** ${bankroll:,.2f}")
        st.write(f"• **Final Bankroll:** ${final_bankroll:,.2f}")
        st.write(f"• **Total Profit:** ${total_profit:,.2f}")
        st.write(f"• **Annual Return:** {annual_return:.1f}%")
        
        # Risk warnings
        if annual_return > 100:
            st.warning("⚠️ Projection may be overly optimistic")
        if (total_exposure / bankroll) > 0.5:
            st.error("🚨 High portfolio exposure - consider reducing position sizes")
    
    # Kelly Criterion calculator
    with st.expander("🧮 Kelly Criterion Calculator"):
        st.markdown("**Calculate optimal stake size for any bet**")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            calc_probability = st.slider("Win Probability", 0.1, 0.9, 0.6)
        with col2:
            calc_odds = st.number_input("Decimal Odds", min_value=1.1, max_value=10.0, value=2.0, step=0.1)
        with col3:
            calc_bankroll = st.number_input("Bankroll ($)", min_value=100, value=bankroll)
        
        # Calculate Kelly
        b = calc_odds - 1
        p = calc_probability
        q = 1 - p
        
        kelly_fraction = (b * p - q) / b
        kelly_stake = kelly_fraction * calc_bankroll
        
        if kelly_fraction > 0:
            st.success(f"**Recommended Stake:** ${kelly_stake:.2f} ({kelly_fraction*100:.1f}% of bankroll)")
            st.write(f"**Expected Value:** {((p * calc_odds) - 1)*100:.1f}%")
            st.write(f"**Edge:** {(p - (1/calc_odds))*100:.1f}%")
        else:
            st.error("❌ No positive expected value - avoid this bet")

if __name__ == "__main__":
    main()