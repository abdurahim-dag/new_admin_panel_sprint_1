--CREATE DATABASE movies_database;
CREATE SCHEMA if not exists content;

SET search_path TO content,public;
ALTER ROLE app SET search_path TO content,public;

CREATE TABLE IF NOT EXISTS content.film_work
(
    id            uuid PRIMARY KEY,
    title         TEXT NOT NULL,
    description   TEXT,
    creation_date DATE,
    rating        FLOAT,
    type          TEXT not null,
    created       timestamp with time zone,
    modified      timestamp with time zone
);

create table IF NOT EXISTS content.genre
(
    id          uuid PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    created     timestamp with time zone,
    modified    timestamp with time zone
);

create table IF NOT EXISTS content.person
(
    id        uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created   timestamp with time zone,
    modified  timestamp with time zone
);

create table if not exists content.genre_film_work
(
    id           uuid PRIMARY KEY,
    film_work_id uuid not null references film_work(id) on delete cascade,
    genre_id     uuid not null references genre(id) on delete cascade,
    created      timestamp with time zone
);

create table if not exists content.person_film_work
(
    id           uuid PRIMARY KEY,
    film_work_id uuid NOT NULL references film_work(id) on delete cascade,
    person_id    uuid NOT NULL references person(id) on delete cascade,
    role         text NOT NULL,
    created      timestamp with time zone
);

create index concurrently if not exists film_work_title_like_idx on content.film_work(title varchar_pattern_ops);
CREATE INDEX concurrently if not exists film_work_title_idx ON content.film_work (title);
CREATE INDEX concurrently if not exists film_work_rating_idx ON content.film_work (rating);
CREATE INDEX concurrently if not exists film_work_creation_date_idx ON content.film_work (creation_date);

CREATE UNIQUE INDEX concurrently if not exists genre_film_work_unique_idx ON content.genre_film_work (genre_id, film_work_id);
CREATE UNIQUE INDEX concurrently if not exists person_film_work_unique_idx ON content.person_film_work (person_id, film_work_id);