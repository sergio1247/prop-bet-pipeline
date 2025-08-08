# Database Setup Instructions

## Files
- `init.sql.zip` (59MB) - Full database with all NBA data
- `schema.sql` - Table structures only (for reference)

## Setup Process

### 1. Extract Database
```bash
unzip init.sql.zip
```

### 2. Start PostgreSQL Container
```bash
docker-compose up -d pg-player
```

### 3. Import Database
```bash
docker exec -i pg-player psql -U sergio -d sportsdb -f /docker-entrypoint-initdb.d/init.sql
```

### 4. Verify Setup
```bash
docker exec pg-player psql -U sergio -d sportsdb -c "SELECT COUNT(*) FROM Players;"
```

## Database Structure
- **Players** - NBA player information
- **PlayerStatistics** - Game-by-game stats  
- **Games** - Game information and dates
- **predictions** - Generated ML predictions

## Notes
- The full database is ~986MB unzipped
- Contains multiple NBA seasons of data
- Required for the prop bet prediction pipeline to work properly