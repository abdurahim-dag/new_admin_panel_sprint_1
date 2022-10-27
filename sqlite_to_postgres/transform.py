""" Модуль трансформации.

Модуль трансформации экспортированного csv файла:
- замена заголовка файла, на заголовок целевой таблицы;
- фильтрация строк не соответствующих строк файла типам полей датакласса.
"""

import csv
import dataclasses
from contextlib import closing
from typing import Union

from pendulum import DateTime
from pendulum import parse

from logger import logger
from tables_target import FilmWork, Genre, Person, PersonFilmWork, GenreFilmWork


class Transform:
    """
    Класс, для организации проверок и
    переименования столбцов файла csv.
    """
    def transform(
        self,
        file_from: str,
        file_to: str,
        dataclass: Union[FilmWork, Genre, Person, PersonFilmWork, GenreFilmWork],
    ) -> None:
        """Выгрузка с проверкой на соответствие типу и
            переименования заголовка файла csv.

        Args:
            file_from (str): Название исходного файла csv.
            file_to (str): Название результирующего файла csv.
            dataclass (dataclass): Описание структуры целевой таблицы.
        """
        # Получаем имена и типы столбцов.
        field_types = {field.name: field.type for field in dataclasses.fields(dataclass)}
        columns_to = field_types.keys()
        with closing(open(file_from, 'r', encoding="utf-8", newline='')) as extracted_file:
            csv_reader = csv.reader(extracted_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
            with closing(open(file_to, 'w', encoding="utf-8", newline='')) as upload_file:
                csv_writer = csv.writer(upload_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
                # Заголовок исходного файла не интересует.
                next(csv_reader, None)
                # Записываем правильный заголовок целевой таблицы
                csv_writer.writerow(columns_to)
                for row in csv_reader:
                    # Создаём датакласс из имён из строки.
                    kwargs = dict(zip(columns_to, row))
                    dc = dataclass(**kwargs)
                    # Проверяем на соответствие значений типу в dataclass.
                    try:
                        for field, typo in field_types.items():
                            value = getattr(dc, field)
                            if bool(value):
                                if typo == DateTime:
                                    parse(value)
                                else:
                                    typo(value)
                    except (ValueError, TypeError) as e:
                        logger.info(
                            f"Value error of id({dc.id}) field({field}) by value ({value}) from file {file_from} - {e}")
                        continue
                    # Если проверка пройдена, то записываем.
                    csv_writer.writerow(row)
                logger.info(f"File {file_from} completed transformation.")