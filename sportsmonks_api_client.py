# %%
# Cell 1: Imports
import os
import json
import time
import requests
from typing import Dict, List, Any, Optional, Union
from datetime import datetime

# %%
# Cell 2: API Client Class
class SportMonksAPI:
    """
    Client for SportMonks API v3
    """
    
    def __init__(self, api_token: str, base_url: str = "https://api.sportmonks.com/v3", rate_limit: int = 3000):
        """
        Initialize the SportMonks API client
        
        Args:
            api_token: Your SportMonks API token
            base_url: Base URL for the API
            rate_limit: Rate limit per hour (default 3000)
        """
        self.api_token = api_token
        self.base_url = base_url
        self.rate_limit = rate_limit
        # Track requests to handle rate limiting
        self.request_count = 0
        self.request_reset_time = time.time() + 3600  # Reset after an hour
        
    def _handle_rate_limit(self):
        """Handle API rate limiting"""
        current_time = time.time()
        
        # Reset counter if an hour has passed
        if current_time > self.request_reset_time:
            self.request_count = 0
            self.request_reset_time = current_time + 3600
            
        # Check if we're approaching the rate limit
        if self.request_count >= self.rate_limit:
            wait_time = self.request_reset_time - current_time
            print(f"Rate limit approaching. Waiting {wait_time:.2f} seconds...")
            time.sleep(wait_time)
            self.request_count = 0
            self.request_reset_time = time.time() + 3600
            
        # Add a small delay between requests to be nice to the API
        time.sleep(0.1)
    
    def make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """
        Make a request to the SportMonks API
        
        Args:
            endpoint: API endpoint (e.g., '/football/leagues')
            params: Additional query parameters
            
        Returns:
            API response as a dictionary
        """
        self._handle_rate_limit()
        
        # Prepare the request
        url = f"{self.base_url}{endpoint}"
        
        # Ensure params is a dictionary
        if params is None:
            params = {}
            
        # Add API token to params
        params['api_token'] = self.api_token
        
        # Make the request
        try:
            response = requests.get(url, params=params)
            self.request_count += 1
            
            # Check for errors
            response.raise_for_status()
            
            # Parse the response
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print(f"API request error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response content: {e.response.content}")
            raise
            
    def fetch_paginated_data(self, endpoint: str, params: Dict = None) -> List[Dict]:
        """
        Fetch all pages of data from a paginated endpoint
        
        Args:
            endpoint: API endpoint
            params: Additional query parameters
            
        Returns:
            List of data items from all pages
        """
        if params is None:
            params = {}
            
        # Set default page size to maximum
        if 'per_page' not in params:
            params['per_page'] = 1000
            
        all_data = []
        page = 1
        total_pages = 1  # Will be updated after first request
        
        while page <= total_pages:
            params['page'] = page
            response = self.make_request(endpoint, params)
            
            # Update total pages if available
            pagination = response.get('pagination', {})
            total_pages = pagination.get('total_pages', 1)
            
            # Extract data
            data = response.get('data', [])
            if not data:
                # No more data
                break
                
            all_data.extend(data)
            page += 1
            
        return all_data
        
    def fetch_core_data(self, entity_type: str) -> List[Dict]:
        """
        Fetch all data for a core entity type
        
        Args:
            entity_type: One of 'continents', 'countries', 'regions', 'cities'
            
        Returns:
            List of entities
        """
        return self.fetch_paginated_data(f"/core/{entity_type}")
    
    def fetch_football_data(self, entity_type: str, params: Dict = None) -> List[Dict]:
        """
        Fetch all data for a football entity type
        
        Args:
            entity_type: Entity type (e.g., 'leagues', 'teams')
            params: Additional query parameters
            
        Returns:
            List of entities
        """
        return self.fetch_paginated_data(f"/football/{entity_type}", params)
        
    def fetch_seasons_by_league(self, league_id: int) -> List[Dict]:
        """
        Fetch all seasons for a specific league
        
        Args:
            league_id: League ID
            
        Returns:
            List of seasons
        """
        params = {
            'filter[league_id]': league_id
        }
        return self.fetch_football_data('seasons', params)
    
    def fetch_teams_by_season(self, season_id: int) -> List[Dict]:
        """
        Fetch all teams for a specific season
        
        Args:
            season_id: Season ID
            
        Returns:
            List of teams
        """
        params = {
            'filter[season_id]': season_id
        }
        return self.fetch_football_data('teams', params)
        
    def fetch_squads(self, team_id: int, season_id: int) -> List[Dict]:
        """
        Fetch squad for a team in a specific season
        
        Args:
            team_id: Team ID
            season_id: Season ID
            
        Returns:
            Squad data
        """
        params = {
            'filter[team_id]': team_id,
            'filter[season_id]': season_id,
            'include': 'player,statistics'
        }
        return self.fetch_football_data('squads', params)
        
    def fetch_fixtures_by_season(self, season_id: int) -> List[Dict]:
        """
        Fetch all fixtures for a season
        
        Args:
            season_id: Season ID
            
        Returns:
            List of fixtures
        """
        # Try to use the schedules endpoint first
        params = {
            'include': 'fixture'
        }
        schedule_data = self.fetch_paginated_data(f"/football/schedules/season/{season_id}", params)
        
        # If no data from schedules, try direct fixtures endpoint
        if not schedule_data:
            params = {
                'filter[season_id]': season_id,
                'include': 'participants;league;venue;state'
            }
            response = self.make_request("/football/fixtures", params)
            return response.get('data', [])
            
        return schedule_data
    
    def fetch_fixture_details(self, fixture_id: int) -> Dict:
        """
        Fetch detailed information for a fixture
        
        Args:
            fixture_id: Fixture ID
            
        Returns:
            Fixture details
        """
        params = {
            'include': 'participants;league;venue;state;scores;events.type;events.period;events.player;statistics.type;sidelined.sideline.player;sidelined.sideline.type;weatherReport'
        }
        return self.make_request(f"/football/fixtures/{fixture_id}", params)
    
    def fetch_fixtures_by_date_range(self, from_date: str, to_date: str) -> List[Dict]:
        """
        Fetch fixtures for a specific date range
        
        Args:
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            List of fixtures
        """
        params = {
            'from': from_date,
            'to': to_date
        }
        response = self.make_request("/football/fixtures/date-range", params)
        return response.get('data', [])

    def fetch_fixture_details_batch(self, fixture_ids: List[int]) -> List[Dict]:
        """
        Fetch detailed information for a batch of fixtures (up to 50)
        
        Args:
            fixture_ids: List of fixture IDs
            
        Returns:
            List of fixture details
        """
        if len(fixture_ids) > 50:
            raise ValueError("Cannot fetch more than 50 fixtures in one batch")
            
        params = {
            'filter[id]': ','.join(map(str, fixture_ids)),
            'include': 'participants;league;venue;state;scores;events.type;events.period;events.player;statistics.type;sidelined.sideline.player;sidelined.sideline.type;weatherReport'
        }
        response = self.make_request("/football/fixtures", params)
        return response.get('data', [])
        
    def fetch_standings_by_season(self, season_id: int) -> List[Dict]:
        """
        Fetch standings for a season
        
        Args:
            season_id: Season ID
            
        Returns:
            Standings data
        """
        params = {
            'include': 'team'
        }
        return self.make_request(f"/football/standings/season/{season_id}", params).get('data', [])
    
    def fetch_topscorers_by_season(self, season_id: int) -> List[Dict]:
        """
        Fetch top scorers for a season
        
        Args:
            season_id: Season ID
            
        Returns:
            Top scorers data
        """
        params = {
            'include': 'player'
        }
        return self.make_request(f"/football/topscorers/season/{season_id}", params).get('data', [])

# %%
# Cell 3: Test the API client
# Replace with your API token
API_TOKEN = "oYeoAVFUTQpu7MfoFqbvyiYfgRRkuBWW0p2atkZnySe4X3xrHkjgGhOvI0pd"

api = SportMonksAPI(API_TOKEN)

# Test a simple API call
leagues = api.fetch_football_data("leagues", {"per_page": 10})
print(f"Found {len(leagues)} leagues:")
for league in leagues:
    print(f"ID: {league.get('id')}, Name: {league.get('name')}")


