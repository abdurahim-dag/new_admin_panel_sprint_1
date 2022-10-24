from target_tables import FilmWork, Genre, Person, GenreFilmWork, PersonFilmWork

map_tables = (
    (
        'film_work',
        ('id', 'title', 'type', 'description', 'rating', 'creation_date', 'created_at', 'updated_at',),
        ('id', 'title', 'type', 'description', 'rating', 'creation_date', 'created', 'modified',),
        FilmWork,
    ),
    (
        'genre',
        ('id', 'name', 'description', 'created_at', 'updated_at'),
        ('id', 'name', 'description', 'created', 'modified'),
        Genre,
    ),
    (
        'person',
        ('id', 'full_name', 'created_at', 'updated_at',),
        ('id', 'full_name', 'created', 'modified',),
        Person
    ),
    (
        'genre_film_work',
        ('id', 'film_work_id', 'genre_id', 'created_at',),
        ('id', 'film_work_id', 'genre_id', 'created',),
        GenreFilmWork
    ),
    (
        'person_film_work',
        ('id', 'film_work_id', 'person_id', 'role', 'created_at'),
        ('id', 'film_work_id', 'person_id', 'role', 'created'),
        PersonFilmWork
    )
)
