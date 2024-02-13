create or replace table leagues  (
  league_id number(2),
  name VARCHAR(100)
);

create or replace table teams  (
  team_id number(2),
  name VARCHAR(100)
);

create or replace table league_teams (
    team_id number(2),
    league_id number(2)
);

create or replace table fixtures (
    fixture_id number(2),
    league_id number(2),
    home_team_id number(2),
    away_team_id number (2),
    home_team_goals number(2),
    away_team_goals number (2),
    fixture_date date
);

