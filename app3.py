import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
import pickle

# Set page configuration
st.set_page_config(page_title="Football Statistics Dashboard", layout="wide", initial_sidebar_state="expanded")

# Connect to the database
def get_connection():
    return sqlite3.connect('db_sportmonks.db')

# --- Load predictions if available ---
@st.cache_data
def load_predictions(file_path='all_leagues_predictions.pkl'):
    if not os.path.exists(file_path):
        return None
    with open(file_path, 'rb') as f:
        preds = pickle.load(f)
    return pd.DataFrame(preds) if isinstance(preds, list) else preds

# Function to get all available leagues
def get_leagues():
    query = """
    SELECT DISTINCT l.id, l.name 
    FROM leagues l
    JOIN fixtures f ON l.id = f.league_id
    WHERE f.score_home IS NOT NULL
    ORDER BY l.name
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)

# Function to get league standings
def get_league_table(league_id):
    query = f"""
    WITH matches AS (
        SELECT 
            f.home_team_id as team_id,
            f.score_home as goals_for,
            f.score_away as goals_against,
            CASE 
                WHEN f.score_home > f.score_away THEN 3
                WHEN f.score_home = f.score_away THEN 1
                ELSE 0
            END as points,
            1 as played,
            CASE WHEN f.score_home > f.score_away THEN 1 ELSE 0 END as win,
            CASE WHEN f.score_home = f.score_away THEN 1 ELSE 0 END as draw,
            CASE WHEN f.score_home < f.score_away THEN 1 ELSE 0 END as loss,
            1 as home_played,
            0 as away_played,
            CASE WHEN f.score_home > f.score_away THEN 1 ELSE 0 END as home_win,
            0 as away_win
        FROM fixtures f
        WHERE f.league_id = {league_id} AND f.score_home IS NOT NULL
        
        UNION ALL
        
        SELECT 
            f.away_team_id as team_id,
            f.score_away as goals_for,
            f.score_home as goals_against,
            CASE 
                WHEN f.score_away > f.score_home THEN 3
                WHEN f.score_away = f.score_home THEN 1
                ELSE 0
            END as points,
            1 as played,
            CASE WHEN f.score_away > f.score_home THEN 1 ELSE 0 END as win,
            CASE WHEN f.score_away = f.score_home THEN 1 ELSE 0 END as draw,
            CASE WHEN f.score_away < f.score_home THEN 1 ELSE 0 END as loss,
            0 as home_played,
            1 as away_played,
            0 as home_win,
            CASE WHEN f.score_away > f.score_home THEN 1 ELSE 0 END as away_win
        FROM fixtures f
        WHERE f.league_id = {league_id} AND f.score_home IS NOT NULL
    )
    
    SELECT 
        t.id as team_id,
        t.name as team,
        SUM(m.played) as played,
        SUM(m.win) as win,
        SUM(m.draw) as draw,
        SUM(m.loss) as loss,
        SUM(m.goals_for) as goals_for,
        SUM(m.goals_against) as goals_against,
        SUM(m.goals_for) - SUM(m.goals_against) as goal_difference,
        SUM(m.points) as points,
        SUM(m.home_played) as home_played,
        SUM(m.home_win) as home_win,
        SUM(m.away_played) as away_played,
        SUM(m.away_win) as away_win
    FROM matches m
    JOIN teams t ON m.team_id = t.id
    GROUP BY t.id, t.name
    ORDER BY points DESC, goal_difference DESC, goals_for DESC
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)
# Function to get team form (last 5 matches)
def get_team_form(team_id, limit=5):
    query = f"""
    SELECT 
        f.starting_at,
        CASE 
            WHEN f.home_team_id = {team_id} THEN at.name
            ELSE ht.name
        END as opponent,
        CASE 
            WHEN f.home_team_id = {team_id} THEN 'H'
            ELSE 'A'
        END as venue,
        CASE 
            WHEN f.home_team_id = {team_id} THEN f.score_home
            ELSE f.score_away
        END as goals_for,
        CASE 
            WHEN f.home_team_id = {team_id} THEN f.score_away
            ELSE f.score_home
        END as goals_against,
        CASE 
            WHEN (f.home_team_id = {team_id} AND f.score_home > f.score_away) OR
                 (f.away_team_id = {team_id} AND f.score_away > f.score_home) THEN 'W'
            WHEN f.score_home = f.score_away THEN 'D'
            ELSE 'L'
        END as result
    FROM fixtures f
    JOIN teams ht ON f.home_team_id = ht.id
    JOIN teams at ON f.away_team_id = at.id
    WHERE (f.home_team_id = {team_id} OR f.away_team_id = {team_id})
    AND f.score_home IS NOT NULL
    ORDER BY f.starting_at DESC
    LIMIT {limit}
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)

def get_recent_results(league_id, limit=20):
    query = f"""
    SELECT 
        f.id as fixture_id,
        f.starting_at,
        ht.name as home_team,
        at.name as away_team,
        f.score_home,
        f.score_away
    FROM fixtures f
    JOIN teams ht ON f.home_team_id = ht.id
    JOIN teams at ON f.away_team_id = at.id
    WHERE f.league_id = {league_id} AND f.score_home IS NOT NULL
    ORDER BY f.starting_at DESC
    LIMIT {limit}
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)

def get_league_goal_stats(league_id):
    query = f"""
    SELECT 
        AVG(f.score_home + f.score_away) as avg_total_goals,
        AVG(f.score_home) as avg_home_goals,
        AVG(f.score_away) as avg_away_goals,
        SUM(CASE WHEN f.score_home + f.score_away > 2.5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as over_2_5_percentage,
        SUM(CASE WHEN f.score_home + f.score_away < 2.5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as under_2_5_percentage,
        SUM(CASE WHEN f.score_home > 0 AND f.score_away > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as btts_percentage,
        COUNT(*) as total_matches
    FROM fixtures f
    WHERE f.league_id = {league_id} AND f.score_home IS NOT NULL
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)

# Create tabs for different view options
def create_sidebar():
    st.sidebar.title("Football Statistics")
    
    # Load predictions if available
    predictions_file = st.sidebar.text_input("Predictions file path (optional)", "all_leagues_predictions.pkl")
    df_predictions = None
    if os.path.exists(predictions_file):
        df_predictions = load_predictions(predictions_file)
        if df_predictions is not None:
            st.sidebar.success(f"Loaded {len(df_predictions)} predictions")
    
    # Get available leagues
    leagues = get_leagues()
    selected_league = st.sidebar.selectbox(
        "Select League",
        options=leagues['id'].tolist(),
        format_func=lambda x: leagues[leagues['id'] == x]['name'].iloc[0]
    )
    
    return selected_league, leagues, df_predictions

# Calculate additional team metrics for league table
def calculate_team_metrics(df):
    df = df.copy()
    df['win_rate'] = df['win'] / df['played'] * 100
    df['home_win_rate'] = df['home_win'] / df['home_played'] * 100
    df['away_win_rate'] = df['away_win'] / df['away_played'] * 100
    df['goals_per_game'] = df['goals_for'] / df['played']
    # Handle division by zero if any
    df = df.replace([np.inf, -np.inf], 0).fillna(0)
    return df

# Display league table tab
def display_league_table(league_id):
    st.subheader("League Standings")
    
    # Get league table
    league_table = get_league_table(league_id)
    league_table = calculate_team_metrics(league_table)
    
    # Add position column
    league_table.insert(0, 'Pos', range(1, len(league_table) + 1))
    
    # Format columns
    formatted_table = league_table[['Pos', 'team', 'played', 'win', 'draw', 'loss', 
                                  'goals_for', 'goals_against', 'goal_difference', 'points']]
    
    # Rename columns for display
    column_rename = {
        'team': 'Team', 
        'played': 'P', 
        'win': 'W', 
        'draw': 'D', 
        'loss': 'L',
        'goals_for': 'GF', 
        'goals_against': 'GA', 
        'goal_difference': 'GD', 
        'points': 'Pts'
    }
    
    formatted_table = formatted_table.rename(columns=column_rename)
    
    # Display as dataframe
    st.dataframe(formatted_table, hide_index=True, use_container_width=True)
    
    return league_table

# Display team statistics tab
def display_team_stats(league_table):
    st.subheader("Team Performance")
    
    # Team selector
    teams = league_table[['team_id', 'team']].values.tolist()
    selected_team_id = st.selectbox(
        "Select Team",
        options=[team[0] for team in teams],
        format_func=lambda x: next((team[1] for team in teams if team[0] == x), "")
    )
    
    # Get team data
    team_data = league_table[league_table['team_id'] == selected_team_id].iloc[0]
    team_name = team_data['team']
    
    # Create columns for team stats
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Position", f"{int(league_table[league_table['team_id'] == selected_team_id]['Pos'].iloc[0])}")
        st.metric("Points", f"{int(team_data['points'])}")
        st.metric("Win Rate", f"{team_data['win_rate']:.1f}%")
    
    with col2:
        st.metric("Goal Difference", f"{int(team_data['goal_difference'])}")
        st.metric("Goals For", f"{int(team_data['goals_for'])}")
        st.metric("Goals Against", f"{int(team_data['goals_against'])}")
    
    with col3:
        st.metric("Home Win Rate", f"{team_data['home_win_rate']:.1f}%")
        st.metric("Away Win Rate", f"{team_data['away_win_rate']:.1f}%")
        st.metric("Goals Per Game", f"{team_data['goals_per_game']:.2f}")
    
    # Display team form
    st.subheader(f"Recent Form: {team_name}")
    form_data = get_team_form(selected_team_id)
    
    if not form_data.empty:
        # Create a container to hold the form items
        form_container = st.container()
        
        # Create content for the form container
        with form_container:
            for _, match in form_data.iterrows():
                date = pd.to_datetime(match['starting_at']).strftime('%Y-%m-%d')
                result = match['result']
                score = f"{match['goals_for']}-{match['goals_against']}"
                venue = "Home" if match['venue'] == 'H' else "Away"
                
                # Set colors based on result
                result_color = {
                    'W': 'green',
                    'D': 'blue',
                    'L': 'red'
                }[result]
                
                # Create a nice looking form item
                st.markdown(
                    f"""
                    <div style='display:flex; align-items:center; margin-bottom:10px;'>
                        <div style='background-color:{result_color}; color:white; padding:8px 12px; border-radius:5px; margin-right:15px; font-weight:bold; width:30px; text-align:center;'>{result}</div>
                        <div style='flex-grow:1;'>
                            <div style='font-size:14px; color:#666;'>{date}</div>
                            <div style='font-weight:bold;'>{team_name} {score} {match['opponent']} ({venue})</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
    else:
        st.info("No recent form data available for this team")

# Display goal statistics tab
def display_goal_stats(league_id, league_table):
    st.subheader("Goal Statistics")
    
    # Get league goal stats
    goal_stats = get_league_goal_stats(league_id)
    
    if not goal_stats.empty:
        stats = goal_stats.iloc[0]
        
        # Create a dashboard of goal stats
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Avg. Goals Per Match", f"{stats['avg_total_goals']:.2f}")
        
        with col2:
            st.metric("Avg. Home Goals", f"{stats['avg_home_goals']:.2f}")
        
        with col3:
            st.metric("Avg. Away Goals", f"{stats['avg_away_goals']:.2f}")
        
        # Create charts for over/under and BTTS
        col1, col2 = st.columns(2)
        
        with col1:
            # Over/Under chart
            fig1, ax1 = plt.subplots(figsize=(6, 4))
            labels = ['Over 2.5', 'Under 2.5']
            sizes = [stats['over_2_5_percentage'], stats['under_2_5_percentage']]
            colors = ['#3498db', '#f1c40f']
            explode = (0.1, 0)
            
            ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
                   shadow=True, startangle=90, colors=colors)
            ax1.axis('equal')
            plt.title('Over/Under 2.5 Goals')
            st.pyplot(fig1)
            
            # Display over/under percentage
            st.metric("Over 2.5 Goals", f"{stats['over_2_5_percentage']:.1f}%")
        
        with col2:
            # BTTS chart
            fig2, ax2 = plt.subplots(figsize=(6, 4))
            labels = ['Both Teams Score', 'Not Both Teams Score']
            sizes = [stats['btts_percentage'], 100 - stats['btts_percentage']]
            colors = ['#2ecc71', '#e74c3c']
            explode = (0.1, 0)
            
            ax2.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
                   shadow=True, startangle=90, colors=colors)
            ax2.axis('equal')
            plt.title('Both Teams To Score')
            st.pyplot(fig2)
            
            # Display BTTS percentage
            st.metric("Both Teams Score", f"{stats['btts_percentage']:.1f}%")
        
        # Team goals comparison
        st.subheader("Teams Scoring & Conceding Comparison")
        
        # Create a dataframe with team scoring metrics
        team_goals = league_table[['team', 'goals_for', 'goals_against', 'played']].copy()
        team_goals['goals_per_game'] = team_goals['goals_for'] / team_goals['played']
        team_goals['conceded_per_game'] = team_goals['goals_against'] / team_goals['played']
        
        # Sort for visualization
        top_scoring = team_goals.sort_values('goals_per_game', ascending=False).head(10)
        
        # Create the visualization
        fig3, ax3 = plt.subplots(figsize=(10, 6))
        
        # Create positions for bars
        x = np.arange(len(top_scoring))
        width = 0.35
        
        # Create bars
        scored = ax3.bar(x - width/2, top_scoring['goals_per_game'], width, label='Goals Scored per Game', color='#3498db')
        conceded = ax3.bar(x + width/2, top_scoring['conceded_per_game'], width, label='Goals Conceded per Game', color='#e74c3c')
        
        # Add labels, title and legend
        ax3.set_ylabel('Goals per Game')
        ax3.set_title('Team Scoring Comparison - Top 10 Scoring Teams')
        ax3.set_xticks(x)
        ax3.set_xticklabels(top_scoring['team'], rotation=45, ha='right')
        ax3.legend()
        
        plt.tight_layout()
        st.pyplot(fig3)
    else:
        st.warning("No goal statistics available for this league")

# Display recent results tab
def display_recent_results(league_id):
    st.subheader("Recent Results")
    
    # Get recent results
    results = get_recent_results(league_id)
    
    if not results.empty:
        # Format and display results
        for _, match in results.iterrows():
            date = pd.to_datetime(match['starting_at']).strftime('%Y-%m-%d')
            
            # Determine winner for styling
            if match['score_home'] > match['score_away']:
                home_style = "font-weight: bold;"
                away_style = ""
                result_color = "#e6f7ff"  # Light blue for home win
            elif match['score_home'] < match['score_away']:
                home_style = ""
                away_style = "font-weight: bold;"
                result_color = "#fff0e6"  # Light orange for away win
            else:
                home_style = ""
                away_style = ""
                result_color = "#f9f9f9"  # Light gray for draw
            
            # Create an HTML card for each match
            st.markdown(
                f"""
                <div style='background-color:{result_color}; padding:10px; border-radius:5px; 
                          margin-bottom:10px; display:flex; align-items:center;'>
                    <div style='width:100px; color:#666;'>{date}</div>
                    <div style='flex-grow:1; text-align:right; {home_style}'>{match['home_team']}</div>
                    <div style='width:60px; text-align:center; font-weight:bold;'>
                        {match['score_home']} - {match['score_away']}
                    </div>
                    <div style='flex-grow:1; text-align:left; {away_style}'>{match['away_team']}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
    else:
        st.info("No recent results available for this league")

# Create a simple visual league table with last 5 form shown
def display_form_table(league_id):
    st.subheader("Form Table")
    
    # Get league table
    league_table = get_league_table(league_id)
    
    # Add position column
    league_table.insert(0, 'Pos', range(1, len(league_table) + 1))
    
    # Get recent form for each team (more efficient bulk approach)
    form_data = {}
    for _, team in league_table.iterrows():
        team_id = team['team_id']
        team_form = get_team_form(team_id)
        form_results = team_form['result'].tolist()
        # Pad with 'N' if fewer than 5 results
        while len(form_results) < 5:
            form_results.append('N')
        form_data[team_id] = form_results
    
    # Create form column in dataframe
    league_table['form'] = league_table['team_id'].map(lambda x: form_data.get(x, ['N', 'N', 'N', 'N', 'N']))
    
    # Create a custom display for the form table
    st.markdown("""
    <style>
    .form-item {
        display: inline-block;
        width: 25px;
        height: 25px;
        border-radius: 50%;
        text-align: center;
        line-height: 25px;
        font-weight: bold;
        margin-right: 5px;
        color: white;
    }
    .form-W { background-color: green; }
    .form-D { background-color: blue; }
    .form-L { background-color: red; }
    .form-N { background-color: gray; }
    </style>
    """, unsafe_allow_html=True)
    
    # Create a container for the form table
    for _, team in league_table.iterrows():
        # Create form indicators
        form_html = ""
        for result in team['form']:
            form_html += f'<div class="form-item form-{result}">{result}</div>'
        
        # Create row with position, team, and form
        st.markdown(
            f"""
            <div style='display:flex; align-items:center; margin-bottom:10px; padding:10px; 
                      background-color:#f5f5f5; border-radius:5px;'>
                <div style='width:30px; font-weight:bold;'>{int(team['Pos'])}</div>
                <div style='flex-grow:1; font-weight:bold;'>{team['team']}</div>
                <div style='width:40px; text-align:center;'>{int(team['points'])}</div>
                <div>{form_html}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

# Main application
def main():
    # Create sidebar for selection
    selected_league, leagues, df_predictions = create_sidebar()
    
    # Get league name
    league_name = leagues[leagues['id'] == selected_league]['name'].iloc[0]
    
    # Create main page header
    st.title(f"{league_name} Statistics")
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "League Table", "Form Table", "Team Statistics", "Goal Statistics", "Recent Results"
    ])
    
    league_table = get_league_table(selected_league)
    league_table = calculate_team_metrics(league_table)
    league_table.insert(0, 'Pos', range(1, len(league_table) + 1))

    
    # Tab 1: League Table
    with tab1:
        display_league_table(selected_league)
    
    # Tab 2: Form Table
    with tab2:
        display_form_table(selected_league)
    
    # Tab 3: Team Statistics
    with tab3:
        display_team_stats(league_table)
    
    # Tab 4: Goal Statistics
    with tab4:
        display_goal_stats(selected_league, league_table)
    
    # Tab 5: Recent Results
    with tab5:
        display_recent_results(selected_league)
    
    # Footer
    st.markdown("---")
    st.markdown(f"Data last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()


