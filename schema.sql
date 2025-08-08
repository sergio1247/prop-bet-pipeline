-- Database Schema for prop-bet-pipeline
-- Contains table structures without data
-- Generated: Thu Aug  7 17:11:15 PDT 2025

-- To restore full database:
-- 1. Unzip init.sql.zip
-- 2. Import: psql -U sergio -d sportsdb -f init.sql


CREATE TABLE IF NOT EXISTS predictions (
    personid INTEGER NOT NULL,
    player_name TEXT,
    stat_type VARCHAR(50) NOT NULL,
    line DOUBLE PRECISION NOT NULL,
    game_date DATE NOT NULL,
    team VARCHAR(10),
    predicted_value DOUBLE PRECISION,
    bet_outcome VARCHAR(10),
    deviation_percentage DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ALTER TABLE predictions ADD COLUMN IF NOT EXISTS probability_over      
-- │   DECIMAL(5,4), ADD COLUMN IF NOT EXISTS confidence_low DECIMAL(8,2), ADD COLUMN IF NOT EXISTS confidence_high                │
-- │   DECIMAL(8,2), ADD COLUMN IF NOT EXISTS confidence_score VARCHAR(10), ADD COLUMN IF NOT EXISTS risk_assessment               │
-- │   VARCHAR(15);

CREATE INDEX IF NOT EXISTS idx_players_names ON Players(firstname, lastname);
CREATE INDEX IF NOT EXISTS idx_players_personid ON Players(personid);
CREATE INDEX IF NOT EXISTS idx_stats_personid ON PlayerStatistics(personid);
