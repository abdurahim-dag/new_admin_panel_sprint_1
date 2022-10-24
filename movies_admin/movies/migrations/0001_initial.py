# Generated by Django 3.2 on 2022-10-23 08:01

import django.core.validators
from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='FilmWork',
            fields=[
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('title', models.CharField(db_index=True, max_length=255, verbose_name='Title')),
                ('description', models.TextField(blank=True, null=True, verbose_name='Description')),
                ('creation_date', models.DateField(blank=True, db_index=True, null=True, verbose_name='Premiere date')),
                ('rating', models.FloatField(blank=True, db_index=True, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(100)], verbose_name='Rating')),
                ('type', models.CharField(choices=[('movie', 'Movie'), ('tv_show', 'Tv show')], default='movie', max_length=255, verbose_name='Type')),
            ],
            options={
                'verbose_name': 'Film work',
                'verbose_name_plural': 'Film works',
                'db_table': 'content"."film_work',
            },
        ),
        migrations.CreateModel(
            name='Genre',
            fields=[
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=255, verbose_name='Name')),
                ('description', models.TextField(blank=True, null=True, verbose_name='Description')),
            ],
            options={
                'verbose_name': 'Genre',
                'verbose_name_plural': 'Genres',
                'db_table': 'content"."genre',
            },
        ),
        migrations.CreateModel(
            name='Person',
            fields=[
                ('created', models.DateTimeField(auto_now_add=True)),
                ('modified', models.DateTimeField(auto_now=True)),
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('full_name', models.CharField(max_length=255, verbose_name='Full name')),
            ],
            options={
                'verbose_name': 'Person',
                'verbose_name_plural': 'Persons',
                'db_table': 'content"."person',
            },
        ),
        migrations.CreateModel(
            name='PersonFilmWork',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('role', models.TextField(verbose_name='Role')),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('film_work', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='movies.filmwork', verbose_name='Film work')),
                ('person', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='movies.person', verbose_name='Person')),
            ],
            options={
                'verbose_name': 'Role',
                'verbose_name_plural': 'Roles',
                'db_table': 'content"."person_film_work',
            },
        ),
        migrations.CreateModel(
            name='GenreFilmWork',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('film_work', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='movies.filmwork', verbose_name='Film work')),
                ('genre', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='movies.genre', verbose_name='Genre')),
            ],
            options={
                'verbose_name': 'Genre',
                'verbose_name_plural': 'Genres',
                'db_table': 'content"."genre_film_work',
            },
        ),
        migrations.AddField(
            model_name='filmwork',
            name='genres',
            field=models.ManyToManyField(through='movies.GenreFilmWork', to='movies.Genre'),
        ),
        migrations.AddField(
            model_name='filmwork',
            name='person',
            field=models.ManyToManyField(through='movies.PersonFilmWork', to='movies.Person'),
        ),
        migrations.AddConstraint(
            model_name='personfilmwork',
            constraint=models.UniqueConstraint(fields=('person_id', 'film_work_id'), name='unique_person_film_work'),
        ),
        migrations.AddConstraint(
            model_name='genrefilmwork',
            constraint=models.UniqueConstraint(fields=('genre_id', 'film_work_id'), name='unique_genre_film_work'),
        ),
    ]
