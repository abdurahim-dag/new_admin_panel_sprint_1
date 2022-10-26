import csv
import dataclasses
from contextlib import closing
from pendulum import DateTime, parse
from logger import logger

class Transform:
    """

    """

    def transform(
            self,
            file_from: str,
            file_to,
            columns_to: tuple,
            dataclass: dataclasses.dataclass):
        """

        """
        field_types = {field.name: field.type for field in dataclasses.fields(dataclass)}
        with closing(open(file_from, 'r', encoding="utf-8", newline='')) as extracted_file:
            csv_reader = csv.reader(extracted_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
            with closing(open(file_to, 'w', encoding="utf-8", newline='')) as upload_file:
                csv_writer = csv.writer(upload_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
                next(csv_reader, None)
                csv_writer.writerow(columns_to)
                for row in csv_reader:
                    kwargs = dict(zip(columns_to, row))
                    dc = dataclass(**kwargs)
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
                    csv_writer.writerow(row)

