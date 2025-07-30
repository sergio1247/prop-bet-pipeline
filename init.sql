CREATE DATABASE airflow_metadata;

-- Switch to sportsdb and create predictions table
\c sportsdb;

create table commonplayerinfo
(
    person_id                        integer not null,
    first_name                       text,
    last_name                        text,
    display_first_last               text,
    display_last_comma_first         text,
    display_fi_last                  text,
    player_slug                      text,
    birthdate                        timestamp,
    school                           text,
    country                          text,
    last_affiliation                 text,
    height                           text,
    weight                           smallint,
    season_exp                       text,
    jersey                           text,
    position                         text,
    rosterstatus                     text,
    games_played_current_season_flag text,
    team_id                          integer,
    team_name                        text,
    team_abbreviation                text,
    team_code                        text,
    team_city                        text,
    playercode                       text,
    from_year                        text,
    to_year                          text,
    dleague_flag                     boolean,
    nba_flag                         boolean,
    games_played_flag                text,
    draft_year                       text,
    draft_round                      text,
    draft_number                     text,
    greatest_75_flag                 boolean
);

create table games
(
    gameid           integer   not null,
    gamedate         timestamp not null,
    gameduration     text,
    hometeamid       integer,
    awayteamid       integer,
    homescore        integer,
    awayscore        integer,
    winner           integer,
    arenaid          integer,
    attendance       integer,
    gametype         text,
    gamelabel        text,
    seriesgamenumber integer,
    gamesublabel     text
);

create table players
(
    personid     integer not null,
    firstname    text,
    lastname     text,
    birthdate    date,
    lastattended text,
    country      text,
    height       integer,
    bodyweight   integer,
    guard        boolean,
    forward      boolean,
    center       boolean,
    draftyear    integer,
    draftround   integer,
    draftnumber  integer
);

create table playerstatistics
(
    id                      serial
        primary key,
    personid                integer not null,
    gameid                  integer not null,
    teamid                  integer not null,
    assists                 integer,
    blocks                  integer,
    fieldgoalsattempted     integer,
    fieldgoalsmade          integer,
    fieldgoalspercentage    double precision,
    foulspersonal           integer,
    freethrowsattempted     integer,
    freethrowsmade          integer,
    freethrowspercentage    double precision,
    numminutes              double precision,
    plusminuspoints         integer,
    points                  integer,
    reboundsdefensive       integer,
    reboundsoffensive       integer,
    reboundstotal           integer,
    steals                  integer,
    threepointersattempted  integer,
    threepointersmade       integer,
    threepointerspercentage double precision,
    turnovers               integer
);

create table teamhistories
(
    teamid           integer not null,
    teamcity         text,
    teamname         text,
    teamabbrev       text,
    seasonfounded    integer,
    seasonactivetill integer,
    league           text
);

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


alter table teamhistories
    owner to sergio;

alter table playerstatistics
    owner to sergio;

alter table players
    owner to sergio;

alter table games
    owner to sergio;

alter table commonplayerinfo
    owner to sergio;
