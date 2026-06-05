import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
conn.autocommit = True
cur = conn.cursor()


def executer(sql: str, description: str) -> None:
    try:
        cur.execute(sql)
        print(f"  ✅ {description}")
    except Exception as e:
        print(f"  ⚠️  {description} — {e}")


print("\n===== CONTRAINTES D'INTÉGRITÉ =====\n")

# ─── CLÉS PRIMAIRES ───────────────────────────────────────────

print("Clés primaires...")

executer("""
    ALTER TABLE marts_players.players
    ADD CONSTRAINT pk_players PRIMARY KEY (player_id);
""", "PK players.player_id")

executer("""
    ALTER TABLE marts_players.clubs
    ADD CONSTRAINT pk_clubs PRIMARY KEY (club_id);
""", "PK clubs.club_id")

executer("""
    ALTER TABLE marts_players.appearances
    ADD CONSTRAINT pk_appearances PRIMARY KEY (appearance_id);
""", "PK appearances.appearance_id")

executer("""
    ALTER TABLE marts_players.player_valuations
    ADD CONSTRAINT pk_valuations PRIMARY KEY (player_id, date);
""", "PK player_valuations (player_id, date)")

executer("""
    ALTER TABLE marts_clubs.unified_matches
    ADD CONSTRAINT pk_unified_matches PRIMARY KEY (match_id);
""", "PK unified_matches.match_id")

executer("""
    ALTER TABLE marts_ml.features_temporal
    ADD CONSTRAINT pk_features_temporal PRIMARY KEY (player_id, date_t0, date_t1);
""", "PK features_temporal (player_id, date_t0, date_t1)")

# ─── CLÉS ÉTRANGÈRES ──────────────────────────────────────────

print("\nClés étrangères...")

executer("""
    ALTER TABLE marts_players.appearances
    ADD CONSTRAINT fk_appearances_player
    FOREIGN KEY (player_id)
    REFERENCES marts_players.players(player_id)
    ON DELETE CASCADE;
""", "FK appearances.player_id → players.player_id")

executer("""
    ALTER TABLE marts_players.player_valuations
    ADD CONSTRAINT fk_valuations_player
    FOREIGN KEY (player_id)
    REFERENCES marts_players.players(player_id)
    ON DELETE CASCADE;
""", "FK player_valuations.player_id → players.player_id")

executer("""
    ALTER TABLE marts_players.transfers
    ADD CONSTRAINT fk_transfers_player
    FOREIGN KEY (player_id)
    REFERENCES marts_players.players(player_id)
    ON DELETE CASCADE;
""", "FK transfers.player_id → players.player_id")

executer("""
    ALTER TABLE marts_ml.features_temporal
    ADD CONSTRAINT fk_features_player
    FOREIGN KEY (player_id)
    REFERENCES marts_players.players(player_id)
    ON DELETE CASCADE;
""", "FK features_temporal.player_id → players.player_id")

executer("""
    ALTER TABLE marts_ml.predictions_market_value_temporal
    ADD CONSTRAINT fk_predictions_player
    FOREIGN KEY (player_id)
    REFERENCES marts_players.players(player_id)
    ON DELETE CASCADE;
""", "FK predictions.player_id → players.player_id")

# ─── CONTRAINTES CHECK ────────────────────────────────────────

print("\nContraintes CHECK...")

executer("""
    ALTER TABLE marts_players.players
    ADD CONSTRAINT chk_players_market_value
    CHECK (market_value_in_eur >= 0);
""", "CHECK players.market_value_in_eur >= 0")

executer("""
    ALTER TABLE marts_players.appearances
    ADD CONSTRAINT chk_appearances_minutes
    CHECK (minutes_played >= 0 AND minutes_played <= 120);
""", "CHECK appearances.minutes_played entre 0 et 120")

executer("""
    ALTER TABLE marts_players.appearances
    ADD CONSTRAINT chk_appearances_goals
    CHECK (goals >= 0);
""", "CHECK appearances.goals >= 0")

executer("""
    ALTER TABLE marts_players.appearances
    ADD CONSTRAINT chk_appearances_assists
    CHECK (assists >= 0);
""", "CHECK appearances.assists >= 0")

executer("""
    ALTER TABLE marts_clubs.unified_matches
    ADD CONSTRAINT chk_matches_home_goals
    CHECK (home_goals >= 0 AND home_goals <= 20);
""", "CHECK unified_matches.home_goals entre 0 et 20")

executer("""
    ALTER TABLE marts_clubs.unified_matches
    ADD CONSTRAINT chk_matches_away_goals
    CHECK (away_goals >= 0 AND away_goals <= 20);
""", "CHECK unified_matches.away_goals entre 0 et 20")

executer("""
    ALTER TABLE marts_players.player_valuations
    ADD CONSTRAINT chk_valuations_value
    CHECK (market_value_in_eur >= 0);
""", "CHECK player_valuations.market_value_in_eur >= 0")

# ─── INDEX ────────────────────────────────────────────────────

print("\nIndex de performance...")

executer("""
    CREATE INDEX IF NOT EXISTS idx_appearances_player_id
    ON marts_players.appearances(player_id);
""", "INDEX appearances(player_id)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_appearances_date
    ON marts_players.appearances(date);
""", "INDEX appearances(date)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_appearances_competition
    ON marts_players.appearances(competition_id);
""", "INDEX appearances(competition_id)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_valuations_player_date
    ON marts_players.player_valuations(player_id, date);
""", "INDEX player_valuations(player_id, date)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_matches_competition_season
    ON marts_clubs.unified_matches(competition, season);
""", "INDEX unified_matches(competition, season)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_matches_date
    ON marts_clubs.unified_matches(date);
""", "INDEX unified_matches(date)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_matches_home_team
    ON marts_clubs.unified_matches(home_team);
""", "INDEX unified_matches(home_team)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_classements_competition_season
    ON marts_clubs.classements_equipes_unified(competition, season);
""", "INDEX classements(competition, season)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_player_perf_player
    ON marts_players.player_performance(player_id);
""", "INDEX player_performance(player_id)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_features_temporal_player
    ON marts_ml.features_temporal(player_id);
""", "INDEX features_temporal(player_id)")

executer("""
    CREATE INDEX IF NOT EXISTS idx_predictions_player
    ON marts_ml.predictions_market_value_temporal(player_id);
""", "INDEX predictions(player_id)")

print("\n✅ Contraintes et index créés.")
cur.close()
conn.close()