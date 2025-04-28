# %%
# Cell 1: Imports
import sqlite3
from typing import Dict, List, Any, Optional, Union
import os
import time
from datetime import datetime

# %%
# Cell 2: Database Manager Class
class SportMonksDB:
    """
    Database manager for SportMonks data
    """
    
    def __init__(self, db_path: str = "sportmonks_football.db"):
        """
        Initialize the database manager
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Access columns by name
        self.cursor = self.conn.cursor()
        
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        
    def execute(self, query: str, params: tuple = ()):
        """Execute a query"""
        return self.cursor.execute(query, params)
        
    def executemany(self, query: str, params_list: List[tuple]):
        """Execute a query with multiple parameter sets"""
        return self.cursor.executemany(query, params_list)
        
    def commit(self):
        """Commit changes"""
        self.conn.commit()

    # Cell 3: Table Creation Methods
    def create_tables(self):
        """Create all tables in the database"""
        # Reference data tables
        self.execute('''
        CREATE TABLE IF NOT EXISTS continents (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT,
            continent_id INTEGER,
            FOREIGN KEY (continent_id) REFERENCES continents (id)
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS regions (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country_id INTEGER,
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS cities (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            region_id INTEGER,
            country_id INTEGER,
            FOREIGN KEY (region_id) REFERENCES regions (id),
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
        ''')
        
        # Venues
        self.execute('''
        CREATE TABLE IF NOT EXISTS venues (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            city_id INTEGER,
            country_id INTEGER,
            capacity INTEGER,
            address TEXT,
            latitude REAL,
            longitude REAL,
            surface TEXT,
            image_path TEXT,
            city_name TEXT,
            national_team INTEGER,
            FOREIGN KEY (city_id) REFERENCES cities (id),
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
        ''')
        
        # States (match status)
        self.execute('''
        CREATE TABLE IF NOT EXISTS states (
            id INTEGER PRIMARY KEY,
            state TEXT,
            name TEXT,
            short_name TEXT,
            developer_name TEXT
        )
        ''')
        
        # Competitions
        self.execute('''
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country_id INTEGER,
            logo_path TEXT,
            slug TEXT,
            type TEXT,
            active INTEGER,
            sub_type TEXT,
            last_played_at TEXT,
            category INTEGER,
            has_jerseys INTEGER,
            sport_id INTEGER,
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS seasons (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            league_id INTEGER,
            start_date TEXT,
            end_date TEXT,
            FOREIGN KEY (league_id) REFERENCES leagues (id)
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS stages (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            season_id INTEGER,
            type TEXT,
            FOREIGN KEY (season_id) REFERENCES seasons (id)
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            stage_id INTEGER,
            FOREIGN KEY (stage_id) REFERENCES stages (id)
        )
        ''')
        
        # Teams
        self.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            country_id INTEGER,
            venue_id INTEGER,
            short_code TEXT,
            founded INTEGER,
            type TEXT,
            placeholder INTEGER,
            gender TEXT,
            logo_path TEXT,
            sport_id INTEGER,
            last_played_at TEXT,
            FOREIGN KEY (country_id) REFERENCES countries (id),
            FOREIGN KEY (venue_id) REFERENCES venues (id)
        )
        ''')
        
        # Players
        self.execute('''
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            common_name TEXT,
            country_id INTEGER,
            nationality_id INTEGER,
            position_id INTEGER,
            detailed_position_id INTEGER,
            type_id INTEGER,
            date_of_birth TEXT,
            height INTEGER,
            weight INTEGER,
            image_path TEXT,
            firstname TEXT,
            lastname TEXT,
            display_name TEXT,
            gender TEXT,
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
        ''')
        
        # Team squads
        self.execute('''
        CREATE TABLE IF NOT EXISTS team_squad (
            team_id INTEGER,
            season_id INTEGER,
            player_id INTEGER,
            jersey_number INTEGER,
            on_loan INTEGER,
            PRIMARY KEY (team_id, season_id, player_id),
            FOREIGN KEY (team_id) REFERENCES teams (id),
            FOREIGN KEY (season_id) REFERENCES seasons (id),
            FOREIGN KEY (player_id) REFERENCES players (id)
        )
        ''')
        
        # Fixtures
        self.execute('''
        CREATE TABLE IF NOT EXISTS fixtures (
            id INTEGER PRIMARY KEY,
            sport_id INTEGER,
            league_id INTEGER,
            season_id INTEGER,
            stage_id INTEGER,
            group_id INTEGER,
            aggregate_id INTEGER,
            round_id INTEGER,
            state_id INTEGER,
            venue_id INTEGER,
            name TEXT,
            starting_at TEXT,
            result_info TEXT,
            leg TEXT,
            details TEXT,
            length INTEGER,
            placeholder INTEGER,
            has_odds INTEGER,
            starting_at_timestamp INTEGER,
            FOREIGN KEY (season_id) REFERENCES seasons (id),
            FOREIGN KEY (league_id) REFERENCES leagues (id),
            FOREIGN KEY (stage_id) REFERENCES stages (id),
            FOREIGN KEY (round_id) REFERENCES rounds (id),
            FOREIGN KEY (state_id) REFERENCES states (id),
            FOREIGN KEY (venue_id) REFERENCES venues (id)
        )
        ''')
        
        # Fixture participants (teams)
        self.execute('''
        CREATE TABLE IF NOT EXISTS fixture_participants (
            fixture_id INTEGER,
            team_id INTEGER,
            location TEXT,
            winner INTEGER,
            position INTEGER,
            PRIMARY KEY (fixture_id, team_id),
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (team_id) REFERENCES teams (id)
        )
        ''')
        
        # Scores
        self.execute('''
        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY,
            fixture_id INTEGER,
            type_id INTEGER,
            participant_id INTEGER,
            goals INTEGER,
            participant TEXT,
            description TEXT,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (participant_id) REFERENCES teams (id)
        )
        ''')
        
        # Event types
        self.execute('''
        CREATE TABLE IF NOT EXISTS event_types (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT,
            developer_name TEXT,
            model_type TEXT,
            stat_group TEXT
        )
        ''')
        
        # Periods
        self.execute('''
        CREATE TABLE IF NOT EXISTS periods (
            id INTEGER PRIMARY KEY,
            fixture_id INTEGER,
            type_id INTEGER,
            started INTEGER,
            ended INTEGER,
            counts_from INTEGER,
            ticking INTEGER,
            sort_order INTEGER,
            description TEXT,
            time_added INTEGER,
            period_length INTEGER,
            minutes INTEGER,
            seconds INTEGER,
            has_timer INTEGER,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id)
        )
        ''')
        
        # Events
        self.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            fixture_id INTEGER,
            period_id INTEGER,
            participant_id INTEGER,
            type_id INTEGER,
            section TEXT,
            player_id INTEGER,
            related_player_id INTEGER,
            player_name TEXT,
            related_player_name TEXT,
            result TEXT,
            info TEXT,
            addition TEXT,
            minute INTEGER,
            extra_minute INTEGER,
            injured INTEGER,
            on_bench INTEGER,
            coach_id INTEGER,
            sub_type_id INTEGER,
            detailed_period_id INTEGER,
            sort_order INTEGER,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (period_id) REFERENCES periods (id),
            FOREIGN KEY (participant_id) REFERENCES teams (id),
            FOREIGN KEY (type_id) REFERENCES event_types (id),
            FOREIGN KEY (player_id) REFERENCES players (id),
            FOREIGN KEY (related_player_id) REFERENCES players (id)
        )
        ''')
        
        # Statistic types
        self.execute('''
        CREATE TABLE IF NOT EXISTS stat_types (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT,
            developer_name TEXT,
            model_type TEXT,
            stat_group TEXT
        )
        ''')
        
        # Fixture team stats
        self.execute('''
        CREATE TABLE IF NOT EXISTS fixture_team_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id INTEGER,
            team_id INTEGER,
            stat_type_id INTEGER,
            value REAL,
            location TEXT,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (team_id) REFERENCES teams (id),
            FOREIGN KEY (stat_type_id) REFERENCES stat_types (id)
        )
        ''')
        
        # Sideline types
        self.execute('''
        CREATE TABLE IF NOT EXISTS sideline_types (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT,
            developer_name TEXT,
            model_type TEXT,
            stat_group TEXT
        )
        ''')
        
        # Sidelines
        self.execute('''
        CREATE TABLE IF NOT EXISTS sidelines (
            id INTEGER PRIMARY KEY,
            player_id INTEGER,
            type_id INTEGER,
            category TEXT,
            team_id INTEGER,
            season_id INTEGER,
            start_date TEXT,
            end_date TEXT,
            games_missed INTEGER,
            completed INTEGER,
            FOREIGN KEY (player_id) REFERENCES players (id),
            FOREIGN KEY (type_id) REFERENCES sideline_types (id),
            FOREIGN KEY (team_id) REFERENCES teams (id),
            FOREIGN KEY (season_id) REFERENCES seasons (id)
        )
        ''')
        
        # Fixture sidelines
        self.execute('''
        CREATE TABLE IF NOT EXISTS fixture_sidelines (
            id INTEGER PRIMARY KEY,
            fixture_id INTEGER,
            sideline_id INTEGER,
            participant_id INTEGER,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (sideline_id) REFERENCES sidelines (id),
            FOREIGN KEY (participant_id) REFERENCES teams (id)
        )
        ''')
        
        # Weather reports
        self.execute('''
        CREATE TABLE IF NOT EXISTS weather_reports (
            id INTEGER PRIMARY KEY,
            fixture_id INTEGER,
            venue_id INTEGER,
            temperature_day REAL,
            temperature_morning REAL,
            temperature_evening REAL,
            temperature_night REAL,
            feels_like_day REAL,
            feels_like_morning REAL,
            feels_like_evening REAL,
            feels_like_night REAL,
            wind_speed REAL,
            wind_direction INTEGER,
            humidity TEXT,
            pressure INTEGER,
            clouds TEXT,
            description TEXT,
            icon TEXT,
            type TEXT,
            metric TEXT,
            current_temp REAL,
            current_wind REAL,
            current_clouds TEXT,
            current_humidity TEXT,
            current_pressure INTEGER,
            current_direction INTEGER,
            current_feels_like REAL,
            current_description TEXT,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (venue_id) REFERENCES venues (id)
        )
        ''')
        
        # Player stats
        self.execute('''
        CREATE TABLE IF NOT EXISTS player_stat_detail (
            player_id INTEGER,
            season_id INTEGER,
            stat_type_id INTEGER,
            value REAL,
            PRIMARY KEY (player_id, season_id, stat_type_id),
            FOREIGN KEY (player_id) REFERENCES players (id),
            FOREIGN KEY (season_id) REFERENCES seasons (id),
            FOREIGN KEY (stat_type_id) REFERENCES stat_types (id)
        )
        ''')
        
        # Fixture player stats
        self.execute('''
        CREATE TABLE IF NOT EXISTS fixture_player_stats (
            fixture_id INTEGER,
            player_id INTEGER,
            stat_type_id INTEGER,
            value REAL,
            PRIMARY KEY (fixture_id, player_id, stat_type_id),
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (player_id) REFERENCES players (id),
            FOREIGN KEY (stat_type_id) REFERENCES stat_types (id)
        )
        ''')
        
        # Standings
        self.execute('''
        CREATE TABLE IF NOT EXISTS standings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_id INTEGER,
            stage_id INTEGER,
            group_id INTEGER,
            round_id INTEGER,
            team_id INTEGER,
            position INTEGER,
            points INTEGER,
            played INTEGER,
            won INTEGER,
            drawn INTEGER,
            lost INTEGER,
            goals_for INTEGER,
            goals_against INTEGER,
            goal_difference INTEGER,
            FOREIGN KEY (season_id) REFERENCES seasons (id),
            FOREIGN KEY (team_id) REFERENCES teams (id)
        )
        ''')
        
        # Top performers
        self.execute('''
        CREATE TABLE IF NOT EXISTS top_performers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            season_id INTEGER,
            category TEXT,
            player_id INTEGER,
            team_id INTEGER,
            value INTEGER,
            rank INTEGER,
            FOREIGN KEY (season_id) REFERENCES seasons (id),
            FOREIGN KEY (player_id) REFERENCES players (id),
            FOREIGN KEY (team_id) REFERENCES teams (id)
        )
        ''')
        
        # Odds tables
        self.execute('''
        CREATE TABLE IF NOT EXISTS bookmakers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            logo TEXT
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            outcome_count INTEGER
        )
        ''')
        
        self.execute('''
        CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fixture_id INTEGER,
            bookmaker_id INTEGER,
            market_id INTEGER,
            outcome TEXT,
            odd_value REAL,
            last_updated TEXT,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id),
            FOREIGN KEY (bookmaker_id) REFERENCES bookmakers (id),
            FOREIGN KEY (market_id) REFERENCES markets (id)
        )
        ''')
        
        # Predictions
        self.execute('''
        CREATE TABLE IF NOT EXISTS predictions (
            fixture_id INTEGER PRIMARY KEY,
            prob_home REAL,
            prob_draw REAL,
            prob_away REAL,
            FOREIGN KEY (fixture_id) REFERENCES fixtures (id)
        )
        ''')
        
        # Metadata table for tracking updates
        self.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        ''')
        
        # Commit the changes
        self.commit()
        
    def create_metadata_table(self):
        """Create the metadata table if it doesn't exist"""
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        self.commit()
    
    # Cell 4: Reference Data Methods
    def insert_or_update_continents(self, continents: List[Dict]):
        """Insert or update continents"""
        for continent in continents:
            self.execute(
                '''
                INSERT OR REPLACE INTO continents (id, name, code)
                VALUES (?, ?, ?)
                ''',
                (continent['id'], continent['name'], continent.get('code'))
            )
        self.commit()
    
    def insert_or_update_countries(self, countries: List[Dict]):
        """Insert or update countries"""
        for country in countries:
            self.execute(
                '''
                INSERT OR REPLACE INTO countries (id, name, code, continent_id)
                VALUES (?, ?, ?, ?)
                ''',
                (
                    country['id'], 
                    country['name'], 
                    country.get('code'),
                    country.get('continent_id')
                )
            )
        self.commit()
    
    def insert_or_update_regions(self, regions: List[Dict]):
        """Insert or update regions"""
        for region in regions:
            self.execute(
                '''
                INSERT OR REPLACE INTO regions (id, name, country_id)
                VALUES (?, ?, ?)
                ''',
                (
                    region['id'], 
                    region['name'], 
                    region.get('country_id')
                )
            )
        self.commit()
    
    def insert_or_update_cities(self, cities: List[Dict]):
        """Insert or update cities"""
        for city in cities:
            self.execute(
                '''
                INSERT OR REPLACE INTO cities (id, name, region_id, country_id)
                VALUES (?, ?, ?, ?)
                ''',
                (
                    city['id'], 
                    city['name'], 
                    city.get('region_id'),
                    city.get('country_id')
                )
            )
        self.commit()

    def insert_or_update_states(self, states: List[Dict]):
        """Insert or update match‐state records (e.g. finished, live, cancelled)"""
        for s in states:
            self.execute(
                '''
                INSERT OR REPLACE INTO states
                  (id, state, name, short_name, developer_name)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    s.get('id'),
                    s.get('state'),
                    s.get('name'),
                    s.get('short_name'),
                    s.get('developer_name'),
                )
            )
        self.commit()

    # Cell 5: Venue and League Methods
    def insert_or_update_venues(self, venues: List[Dict]):
        """Insert or update venues"""
        for venue in venues:
            # If venue contains city data, make sure it's in the cities table
            if 'city' in venue and venue['city']:
                city = venue['city']
                self.execute(
                    '''
                    INSERT OR IGNORE INTO cities (id, name, region_id, country_id)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (
                        city['id'], 
                        city['name'], 
                        city.get('region_id'),
                        city.get('country_id')
                    )
                )
            
            self.execute(
                '''
                INSERT OR REPLACE INTO venues (id, name, city_id, country_id, capacity, address, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    venue['id'], 
                    venue['name'], 
                    venue.get('city_id') or (venue['city']['id'] if 'city' in venue and venue['city'] else None),
                    venue.get('country_id'),
                    venue.get('capacity'),
                    venue.get('address'),
                    venue.get('latitude'),
                    venue.get('longitude')
                )
            )
        self.commit()
    
    def insert_or_update_leagues(self, leagues: List[Dict]):
        """Insert or update leagues"""
        for league in leagues:
            self.execute(
                '''
                INSERT OR REPLACE INTO leagues (id, name, country_id, logo_path, slug, type)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (
                    league['id'], 
                    league['name'], 
                    league.get('country_id'),
                    league.get('logo_path'),
                    league.get('slug'),
                    league.get('type')
                )
            )
        self.commit()
    
    def insert_or_update_seasons(self, seasons: List[Dict]):
        """Insert or update seasons"""
        for season in seasons:
            self.execute(
                '''
                INSERT OR REPLACE INTO seasons (id, name, league_id, start_date, end_date)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    season['id'], 
                    season['name'], 
                    season.get('league_id'),
                    season.get('start_date'),
                    season.get('end_date')
                )
            )
        self.commit()

    # Cell 6: Team and Player Methods
    def insert_or_update_teams(self, teams: List[Dict]):
        """Insert or update teams"""
        for team in teams:
            self.execute(
                '''
                INSERT OR REPLACE INTO teams (id, name, country_id, venue_id, short_code, founded, type, placeholder, gender, logo_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    team['id'], 
                    team['name'], 
                    team.get('country_id'),
                    team.get('venue_id'),
                    team.get('short_code'),
                    team.get('founded'),
                    team.get('type'),
                    team.get('placeholder', 0),
                    team.get('gender'),
                    team.get('logo_path')
                )
            )
        self.commit()
    
    def insert_or_update_players(self, players: List[Dict]):
        """Insert or update players"""
        for player in players:
            self.execute(
                '''
                INSERT OR REPLACE INTO players (id, name, common_name, country_id, position_id, date_of_birth, height, weight, image_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    player['id'], 
                    player['name'],
                    player.get('common_name'),
                    player.get('country_id'),
                    player.get('position_id'),
                    player.get('date_of_birth'),
                    player.get('height'),
                    player.get('weight'),
                    player.get('image_path')
                )
            )
        self.commit()
    
    def insert_or_update_team_squad(self, team_id: int, season_id: int, squad_data: List[Dict]):
        """Insert or update team squad"""
        for item in squad_data:
            player = item.get('player', {})
            if player:
                # Make sure player is in the players table
                self.execute(
                    '''
                    INSERT OR IGNORE INTO players (id, name, common_name, country_id, position_id, date_of_birth, height, weight, image_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        player['id'], 
                        player['name'],
                        player.get('common_name'),
                        player.get('country_id'),
                        player.get('position_id'),
                        player.get('date_of_birth'),
                        player.get('height'),
                        player.get('weight'),
                        player.get('image_path')
                    )
                )
                
                # Insert into team_squad
                self.execute(
                    '''
                    INSERT OR REPLACE INTO team_squad (team_id, season_id, player_id, jersey_number, on_loan)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (
                        team_id,
                        season_id,
                        player['id'],
                        item.get('jersey_number'),
                        item.get('on_loan', 0)
                    )
                )
                
                # Handle player statistics if present
                if 'statistics' in player:
                    for stat in player['statistics']:
                        # Ensure stat type exists
                        self.execute(
                            '''
                            INSERT OR IGNORE INTO stat_types (id, name, code)
                            VALUES (?, ?, ?)
                            ''',
                            (
                                stat['type']['id'],
                                stat['type']['name'],
                                stat['type'].get('code')
                            )
                        )
                        
                        # Insert player stat
                        self.execute(
                            '''
                            INSERT OR REPLACE INTO player_stat_detail (player_id, season_id, stat_type_id, value)
                            VALUES (?, ?, ?, ?)
                            ''',
                            (
                                player['id'],
                                season_id,
                                stat['type']['id'],
                                stat.get('value', 0)
                            )
                        )
        self.commit()        
    # Cell 7: Fixture Methods
    def insert_or_update_fixtures(self, fixtures: List[Dict]):
        """Insert or update multiple fixtures"""
        for fixture in fixtures:
            self.insert_or_update_fixture(fixture)
        self.commit()
        
    def insert_or_update_fixture(self, fixture_data: Dict):
        """Insert or update a single fixture with all related data"""
        # First, make sure we have the basic fixture record
        self.execute(
            '''
            INSERT OR REPLACE INTO fixtures (
                id, sport_id, league_id, season_id, stage_id, group_id, 
                aggregate_id, round_id, state_id, venue_id, name, 
                starting_at, result_info, leg, details, length, 
                placeholder, has_odds, starting_at_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                fixture_data.get('id'),
                fixture_data.get('sport_id'),
                fixture_data.get('league_id'),
                fixture_data.get('season_id'),
                fixture_data.get('stage_id'),
                fixture_data.get('group_id'),
                fixture_data.get('aggregate_id'),
                fixture_data.get('round_id'),
                fixture_data.get('state_id'),
                fixture_data.get('venue_id'),
                fixture_data.get('name'),
                fixture_data.get('starting_at'),
                fixture_data.get('result_info'),
                fixture_data.get('leg'),
                fixture_data.get('details'),
                fixture_data.get('length'),
                1 if fixture_data.get('placeholder') else 0,
                1 if fixture_data.get('has_odds') else 0,
                fixture_data.get('starting_at_timestamp')
            )
        )
        
        # Insert league if included
        if 'league' in fixture_data and fixture_data['league']:
            league = fixture_data['league']
            self.execute(
                '''
                INSERT OR REPLACE INTO leagues (
                    id, sport_id, country_id, name, active, short_code, 
                    image_path, type, sub_type, last_played_at, category, has_jerseys
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    league.get('id'),
                    league.get('sport_id'),
                    league.get('country_id'),
                    league.get('name'),
                    1 if league.get('active') else 0,
                    league.get('short_code'),
                    league.get('image_path'),
                    league.get('type'),
                    league.get('sub_type'),
                    league.get('last_played_at'),
                    league.get('category'),
                    1 if league.get('has_jerseys') else 0
                )
            )
        
        # Insert venue if included
        if 'venue' in fixture_data and fixture_data['venue']:
            venue = fixture_data['venue']
            self.execute(
                '''
                INSERT OR REPLACE INTO venues (
                    id, country_id, city_id, name, address, zipcode,
                    latitude, longitude, capacity, image_path, city_name,
                    surface, national_team
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    venue.get('id'),
                    venue.get('country_id'),
                    venue.get('city_id'),
                    venue.get('name'),
                    venue.get('address'),
                    venue.get('zipcode'),
                    venue.get('latitude'),
                    venue.get('longitude'),
                    venue.get('capacity'),
                    venue.get('image_path'),
                    venue.get('city_name'),
                    venue.get('surface'),
                    1 if venue.get('national_team') else 0
                )
            )
        
        # Insert state if included
        if 'state' in fixture_data and fixture_data['state']:
            state = fixture_data['state']
            self.execute(
                '''
                INSERT OR REPLACE INTO states (
                    id, state, name, short_name, developer_name
                )
                VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    state.get('id'),
                    state.get('state'),
                    state.get('name'),
                    state.get('short_name'),
                    state.get('developer_name')
                )
            )
        
        # Insert participants (teams)
        if 'participants' in fixture_data and fixture_data['participants']:
            for participant in fixture_data['participants']:
                # Insert team
                self.execute(
                    '''
                    INSERT OR REPLACE INTO teams (
                        id, sport_id, country_id, venue_id, gender, name, 
                        short_code, image_path, founded, type, 
                        placeholder, last_played_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        participant.get('id'),
                        participant.get('sport_id'),
                        participant.get('country_id'),
                        participant.get('venue_id'),
                        participant.get('gender'),
                        participant.get('name'),
                        participant.get('short_code'),
                        participant.get('image_path'),
                        participant.get('founded'),
                        participant.get('type'),
                        1 if participant.get('placeholder') else 0,
                        participant.get('last_played_at')
                    )
                )
                
                # Insert fixture_participants entry
                meta = participant.get('meta', {})
                self.execute(
                    '''
                    INSERT OR REPLACE INTO fixture_participants (
                        fixture_id, team_id, location, winner, position
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (
                        fixture_data.get('id'),
                        participant.get('id'),
                        meta.get('location'),
                        1 if meta.get('winner') else 0,
                        meta.get('position')
                    )
                )
        
        # Insert scores
        if 'scores' in fixture_data and fixture_data['scores']:
            for score in fixture_data['scores']:
                score_data = score.get('score', {})
                self.execute(
                    '''
                    INSERT OR REPLACE INTO scores (
                        id, fixture_id, type_id, participant_id, goals,
                        participant, description
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        score.get('id'),
                        score.get('fixture_id'),
                        score.get('type_id'),
                        score.get('participant_id'),
                        score_data.get('goals'),
                        score_data.get('participant'),
                        score.get('description')
                    )
                )

        # Cell 8: Event Processing Methods
        # Insert events
        if 'events' in fixture_data and fixture_data['events']:
            for event in fixture_data['events']:
                # Handle event types
                if 'type' in event and event['type']:
                    event_type = event['type']
                    self.execute(
                        '''
                        INSERT OR REPLACE INTO event_types (
                            id, name, code, developer_name, model_type, stat_group
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        ''',
                        (
                            event_type.get('id'),
                            event_type.get('name'),
                            event_type.get('code'),
                            event_type.get('developer_name'),
                            event_type.get('model_type'),
                            event_type.get('stat_group')
                        )
                    )
                
                # Handle periods
                if 'period' in event and event['period']:
                    period = event['period']
                    self.execute(
                        '''
                        INSERT OR REPLACE INTO periods (
                            id, fixture_id, type_id, started, ended, counts_from,
                            ticking, sort_order, description, time_added,
                            period_length, minutes, seconds, has_timer
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        (
                            period.get('id'),
                            period.get('fixture_id'),
                            period.get('type_id'),
                            period.get('started'),
                            period.get('ended'),
                            period.get('counts_from'),
                            1 if period.get('ticking') else 0,
                            period.get('sort_order'),
                            period.get('description'),
                            period.get('time_added'),
                            period.get('period_length'),
                            period.get('minutes'),
                            period.get('seconds'),
                            1 if period.get('has_timer') else 0
                        )
                    )
                
                # Handle players
                if 'player' in event and event['player']:
                    player = event['player']
                    self.execute(
                        '''
                        INSERT OR REPLACE INTO players (
                            id, sport_id, country_id, nationality_id, city_id,
                            position_id, detailed_position_id, type_id, common_name,
                            firstname, lastname, name, display_name, image_path,
                            height, weight, date_of_birth, gender
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        (
                            player.get('id'),
                            player.get('sport_id'),
                            player.get('country_id'),
                            player.get('nationality_id'),
                            player.get('city_id'),
                            player.get('position_id'),
                            player.get('detailed_position_id'),
                            player.get('type_id'),
                            player.get('common_name'),
                            player.get('firstname'),
                            player.get('lastname'),
                            player.get('name'),
                            player.get('display_name'),
                            player.get('image_path'),
                            player.get('height'),
                            player.get('weight'),
                            player.get('date_of_birth'),
                            player.get('gender')
                        )
                    )
                
                # Insert the event record
                self.execute(
                    '''
                    INSERT OR REPLACE INTO events (
                        id, fixture_id, period_id, participant_id, type_id,
                        section, player_id, related_player_id, player_name,
                        related_player_name, result, info, addition, minute,
                        extra_minute, injured, on_bench, coach_id, sub_type_id,
                        detailed_period_id, sort_order
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        event.get('id'),
                        event.get('fixture_id'),
                        event.get('period_id'),
                        event.get('participant_id'),
                        event.get('type_id'),
                        event.get('section'),
                        event.get('player_id'),
                        event.get('related_player_id'),
                        event.get('player_name'),
                        event.get('related_player_name'),
                        event.get('result'),
                        event.get('info'),
                        event.get('addition'),
                        event.get('minute'),
                        event.get('extra_minute'),
                        1 if event.get('injured') else 0,
                        1 if event.get('on_bench') else 0,
                        event.get('coach_id'),
                        event.get('sub_type_id'),
                        event.get('detailed_period_id'),
                        event.get('sort_order')
                    )
                )

            # Cell 9: Statistics Processing Methods
        # Insert statistics
        if 'statistics' in fixture_data and fixture_data['statistics']:
            for stat in fixture_data['statistics']:
                # Handle stat types
                if 'type' in stat and stat['type']:
                    stat_type = stat['type']
                    self.execute(
                        '''
                        INSERT OR REPLACE INTO stat_types (
                            id, name, code, developer_name, model_type, stat_group
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                        ''',
                        (
                            stat_type.get('id'),
                            stat_type.get('name'),
                            stat_type.get('code'),
                            stat_type.get('developer_name'),
                            stat_type.get('model_type'),
                            stat_type.get('stat_group')
                        )
                    )
                
                # Insert the team stat
                value = stat.get('data', {}).get('value')
                self.execute(
                    '''
                    INSERT OR REPLACE INTO fixture_team_stats (
                        id, fixture_id, team_id, stat_type_id, value, location
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        stat.get('id'),
                        stat.get('fixture_id'),
                        stat.get('participant_id'),
                        stat.get('type_id'),
                        value,
                        stat.get('location')
                    )
                )

        # Cell 10: Sidelined Player Methods
        # Insert sidelined (injured/suspended players)
        if 'sidelined' in fixture_data and fixture_data['sidelined']:
            for sidelined in fixture_data['sidelined']:
                # Insert the fixture_sidelines entry
                self.execute(
                    '''
                    INSERT OR REPLACE INTO fixture_sidelines (
                        id, fixture_id, sideline_id, participant_id
                    )
                    VALUES (?, ?, ?, ?)
                    ''',
                    (
                        sidelined.get('id'),
                        sidelined.get('fixture_id'),
                        sidelined.get('sideline_id'),
                        sidelined.get('participant_id')
                    )
                )
                
                # Handle the sideline details
                if 'sideline' in sidelined and sidelined['sideline']:
                    sideline = sidelined['sideline']
                    
                    # Handle sideline types
                    if 'type' in sideline and sideline['type']:
                        sideline_type = sideline['type']
                        self.execute(
                            '''
                            INSERT OR REPLACE INTO sideline_types (
                                id, name, code, developer_name, model_type, stat_group
                            )
                            VALUES (?, ?, ?, ?, ?, ?)
                            ''',
                            (
                                sideline_type.get('id'),
                                sideline_type.get('name'),
                                sideline_type.get('code'),
                                sideline_type.get('developer_name'),
                                sideline_type.get('model_type'),
                                sideline_type.get('stat_group')
                            )
                        )
                    
                    # Handle players
                    if 'player' in sideline and sideline['player']:
                        player = sideline['player']
                        self.execute(
                            '''
                            INSERT OR REPLACE INTO players (
                                id, sport_id, country_id, nationality_id, city_id,
                                position_id, detailed_position_id, type_id, common_name,
                                firstname, lastname, name, display_name, image_path,
                                height, weight, date_of_birth, gender
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''',
                            (
                                player.get('id'),
                                player.get('sport_id'),
                                player.get('country_id'),
                                player.get('nationality_id'),
                                player.get('city_id'),
                                player.get('position_id'),
                                player.get('detailed_position_id'),
                                player.get('type_id'),
                                player.get('common_name'),
                                player.get('firstname'),
                                player.get('lastname'),
                                player.get('name'),
                                player.get('display_name'),
                                player.get('image_path'),
                                player.get('height'),
                                player.get('weight'),
                                player.get('date_of_birth'),
                                player.get('gender')
                            )
                        )
                    
                    # Insert the sideline record
                    self.execute(
                        '''
                        INSERT OR REPLACE INTO sidelines (
                            id, player_id, type_id, category, team_id,
                            season_id, start_date, end_date, games_missed, completed
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''',
                        (
                            sideline.get('id'),
                            sideline.get('player_id'),
                            sideline.get('type_id'),
                            sideline.get('category'),
                            sideline.get('team_id'),
                            sideline.get('season_id'),
                            sideline.get('start_date'),
                            sideline.get('end_date'),
                            sideline.get('games_missed'),
                            1 if sideline.get('completed') else 0
                        )
                    )
        # Cell 11: Weather Report Methods
        # Insert weather report
        if 'weatherreport' in fixture_data and fixture_data['weatherreport']:
            weather = fixture_data['weatherreport']
            temperature = weather.get('temperature', {})
            feels_like = weather.get('feels_like', {})
            wind = weather.get('wind', {})
            current = weather.get('current', {})
            
            self.execute(
                '''
                INSERT OR REPLACE INTO weather_reports (
                    id, fixture_id, venue_id, temperature_day, temperature_morning,
                    temperature_evening, temperature_night, feels_like_day,
                    feels_like_morning, feels_like_evening, feels_like_night,
                    wind_speed, wind_direction, humidity, pressure, clouds,
                    description, icon, type, metric, current_temp, current_wind,
                    current_clouds, current_humidity, current_pressure,
                    current_direction, current_feels_like, current_description
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    weather.get('id'),
                    weather.get('fixture_id'),
                    weather.get('venue_id'),
                    temperature.get('day'),
                    temperature.get('morning'),
                    temperature.get('evening'),
                    temperature.get('night'),
                    feels_like.get('day'),
                    feels_like.get('morning'),
                    feels_like.get('evening'),
                    feels_like.get('night'),
                    wind.get('speed'),
                    wind.get('direction'),
                    weather.get('humidity'),
                    weather.get('pressure'),
                    weather.get('clouds'),
                    weather.get('description'),
                    weather.get('icon'),
                    weather.get('type'),
                    weather.get('metric'),
                    current.get('temp'),
                    current.get('wind'),
                    current.get('clouds'),
                    current.get('humidity'),
                    current.get('pressure'),
                    current.get('direction'),
                    current.get('feels_like'),
                    current.get('description')
                )
            )
        
        # Commit after processing the entire fixture
        self.commit()


    # Cell 12: Standing and Performer Methods
    def insert_or_update_standings(self, season_id: int, standings: List[Dict]):
        """Insert or update standings for a season"""
        # Clear existing standings for this season
        self.execute("DELETE FROM standings WHERE season_id = ?", (season_id,))
        
        for standing in standings:
            team = standing.get('team', {})
            self.execute(
                '''
                INSERT OR IGNORE INTO teams (id, name, logo_path)
                VALUES (?, ?, ?)
                ''',
                (
                    team.get('id'),
                    team.get('name'),
                    team.get('logo_path')
                )
            )
            
            self.execute(
                '''
                INSERT INTO standings (
                    season_id, stage_id, group_id, round_id, team_id, position,
                    points, played, won, drawn, lost, goals_for, goals_against, goal_difference
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    season_id,
                    standing.get('stage_id'),
                    standing.get('group_id'),
                    standing.get('round_id'),
                    team.get('id'),
                    standing.get('position'),
                    standing.get('points'),
                    standing.get('overall', {}).get('games_played'),
                    standing.get('overall', {}).get('won'),
                    standing.get('overall', {}).get('draw'),
                    standing.get('overall', {}).get('lost'),
                    standing.get('overall', {}).get('goals_scored'),
                    standing.get('overall', {}).get('goals_against'),
                    standing.get('overall', {}).get('goal_difference')
                )
            )
        self.commit()
        
    def insert_or_update_top_performers(self, season_id: int, category: str, performers: List[Dict]):
        """Insert or update top performers for a season"""
        for rank, performer in enumerate(performers, 1):
            player = performer.get('player', {})
            self.execute(
                '''
                INSERT OR IGNORE INTO players (id, name, image_path)
                VALUES (?, ?, ?)
                ''',
                (
                    player.get('id'),
                    player.get('name'),
                    player.get('image_path')
                )
            )
            
            self.execute(
                '''
                INSERT OR REPLACE INTO top_performers (
                    season_id, category, player_id, team_id, value, rank
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (
                    season_id,
                    category,
                    player.get('id'),
                    performer.get('team_id'),
                    performer.get('goals', 0) if category == 'goals' else 
                    performer.get('assists', 0) if category == 'assists' else
                    performer.get('cards', 0) if category == 'cards' else 0,
                    rank
                )
            )
        self.commit()

    # Cell 13: Odds and Predictions Methods
    def insert_or_update_bookmakers(self, bookmakers: List[Dict]):
        """Insert or update bookmakers"""
        for bookmaker in bookmakers:
            self.execute(
                '''
                INSERT OR REPLACE INTO bookmakers (id, name, logo)
                VALUES (?, ?, ?)
                ''',
                (
                    bookmaker['id'], 
                    bookmaker['name'], 
                    bookmaker.get('logo')
                )
            )
        self.commit()
        
    def insert_or_update_markets(self, markets: List[Dict]):
        """Insert or update markets"""
        for market in markets:
            self.execute(
                '''
                INSERT OR REPLACE INTO markets (id, name, outcome_count)
                VALUES (?, ?, ?)
                ''',
                (
                    market['id'], 
                    market['name'], 
                    market.get('outcome_count')
                )
            )
        self.commit()
        
    def insert_or_update_odds(self, fixture_id: int, odds_data: List[Dict]):
        """Insert or update odds for a fixture"""
        # Clear existing odds for this fixture
        self.execute("DELETE FROM odds WHERE fixture_id = ?", (fixture_id,))
        
        for odd in odds_data:
            bookmaker = odd.get('bookmaker', {})
            market = odd.get('market', {})
            
            # Ensure bookmaker exists
            self.execute(
                '''
                INSERT OR IGNORE INTO bookmakers (id, name, logo)
                VALUES (?, ?, ?)
                ''',
                (
                    bookmaker.get('id'),
                    bookmaker.get('name'),
                    bookmaker.get('logo')
                )
            )
            
            # Ensure market exists
            self.execute(
                '''
                INSERT OR IGNORE INTO markets (id, name, outcome_count)
                VALUES (?, ?, ?)
                ''',
                (
                    market.get('id'),
                    market.get('name'),
                    market.get('outcome_count', 0)
                )
            )
            
            # Insert odds
            for outcome in odd.get('outcomes', []):
                self.execute(
                    '''
                    INSERT INTO odds (
                        fixture_id, bookmaker_id, market_id, outcome, odd_value, last_updated
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        fixture_id,
                        bookmaker.get('id'),
                        market.get('id'),
                        outcome.get('name'),
                        outcome.get('value'),
                        odd.get('last_updated')
                    )
                )
        self.commit()
        
    def insert_or_update_predictions(self, fixture_id: int, predictions: Dict):
        """Insert or update predictions for a fixture"""
        self.execute(
            '''
            INSERT OR REPLACE INTO predictions (fixture_id, prob_home, prob_draw, prob_away)
            VALUES (?, ?, ?, ?)
            ''',
            (
                fixture_id,
                predictions.get('predictions', {}).get('home'),
                predictions.get('predictions', {}).get('draw'),
                predictions.get('predictions', {}).get('away')
            )
        )
        self.commit()    

    # Cell 14: Query Methods
    def get_all_leagues(self):
        """Get all leagues from the database"""
        self.execute("SELECT id, name FROM leagues")
        return self.cursor.fetchall()
        
    def get_seasons_by_league(self, league_id, limit=5):
        """Get the most recent seasons for a league"""
        self.execute(
            """
            SELECT id, name, start_date, end_date
            FROM seasons
            WHERE league_id = ?
            ORDER BY start_date DESC
            LIMIT ?
            """, 
            (league_id, limit)
        )
        return self.cursor.fetchall()
        
    def get_teams_by_season(self, season_id):
        """Get all teams that participated in a season"""
        self.execute(
            """
            SELECT DISTINCT t.id, t.name
            FROM teams t
            JOIN fixture_participants fp ON t.id = fp.team_id
            JOIN fixtures f ON fp.fixture_id = f.id
            WHERE f.season_id = ?
            """,
            (season_id,)
        )
        return self.cursor.fetchall()
        
    def get_fixtures_by_season(self, season_id):
        """Get all fixtures for a season"""
        self.execute(
            """
            SELECT id, starting_at, state_id
            FROM fixtures
            WHERE season_id = ?
            """,
            (season_id,)
        )
        return self.cursor.fetchall()
        
    def get_last_update_time(self, entity_type):
        """Get the last update time for an entity type"""
        self.execute(
            """
            SELECT value
            FROM metadata
            WHERE key = ?
            """,
            (f"last_update_{entity_type}",)
        )
        result = self.cursor.fetchone()
        return result[0] if result else None
        
    def set_last_update_time(self, entity_type, timestamp=None):
        """Set the last update time for an entity type"""
        if timestamp is None:
            timestamp = datetime.now().isoformat()
            
        self.execute(
            """
            INSERT OR REPLACE INTO metadata (key, value)
            VALUES (?, ?)
            """,
            (f"last_update_{entity_type}", timestamp)
        )
        self.commit()    

# %%
# Cell 15: Test the Database Manager
if __name__ == "__main__":
    # Create a test database
    db_path = "sportmonks_test.db"
    
    # Delete if exists
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create a new database
    db = SportMonksDB(db_path)
    
    with db:
        # Create tables
        db.create_tables()
        
        # Test inserting some data
        db.insert_or_update_continents([
            {"id": 1, "name": "Europe", "code": "EU"},
            {"id": 2, "name": "Asia", "code": "AS"}
        ])
        
        # Test querying
        db.execute("SELECT * FROM continents")
        continents = db.cursor.fetchall()
        print(f"Inserted {len(continents)} continents:")
        for continent in continents:
            print(f"ID: {continent['id']}, Name: {continent['name']}, Code: {continent['code']}")
        
    print(f"Database created at {db_path}")


