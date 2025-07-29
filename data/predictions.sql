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
