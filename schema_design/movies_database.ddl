--CREATE DATABASE movies_database;
CREATE SCHEMA if not exists content;

SET search_path TO content,public;
ALTER ROLE app SET search_path TO content,public;

CREATE TABLE IF NOT EXISTS content.film_work (
                                                 id uuid PRIMARY KEY,
                                                 title TEXT NOT NULL,
                                                 description TEXT,
                                                 creation_date DATE,
                                                 rating FLOAT,
                                                 type TEXT not null,
                                                 created timestamp with time zone,
                                                 modified timestamp with time zone
);

create table IF NOT EXISTS genre(
                                    id uuid PRIMARY KEY,
                                    name TEXT NOT NULL,
                                    description TEXT,
                                    created timestamp with time zone,
                                    modified timestamp with time zone
);

create table if not exists genre_film_work
(
    id uuid PRIMARY KEY,
    genre_id uuid,
    film_work_id uuid,
    created timestamp with time zone,
    foreign key (genre_id) references genre(id),
    foreign key (film_work_id) references film_work(id)
);

create table IF NOT EXISTS person(
                                     id uuid PRIMARY KEY,
                                     full_name TEXT NOT NULL,
                                     created timestamp with time zone,
                                     modified timestamp with time zone
);

create table if not exists person_film_work(
                                               id uuid PRIMARY KEY,
                                               person_id uuid,
                                               film_work_id uuid,
                                               role text NOT NULL,
                                               created timestamp with time zone,
                                               foreign key (person_id) references person(id),
                                               foreign key (film_work_id) references film_work(id)
);
