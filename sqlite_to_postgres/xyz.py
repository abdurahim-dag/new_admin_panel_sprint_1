import sqlite3

# Задаём путь к файлу с базой данных
db_path = 'db.sqlite'
# Устанавливаем соединение с БД
conn = sqlite3.connect(db_path)
# По-умолчанию SQLite возвращает строки в виде кортежа значений. Эта строка указывает, что данные должны быть в формате «ключ-значение»
conn.row_factory = sqlite3.Row
# Получаем курсор
curs = conn.cursor()
# Формируем запрос. Внутри execute находится обычный SQL-запрос
curs.execute("SELECT * FROM film_work;")
# Получаем данные
data = curs.fetchall()
# Рассматриваем первую запись
print(dict(data[0]))
# Разрываем соединение с БД
conn.close()