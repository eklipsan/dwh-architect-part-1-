# Создание staging слоя в PostrgreSQL

1. Создать stagin слой и подключить его источнику данных, используя расширение postgre_fwd.

2. Создать в staging слое таблицы для загрузки данных из таблиц:
- film
- inventory
- rental
- payment

3. Сделать процедуры для загрузки данных из источника в staging слой в таблицы:
- film
- inventory
- rental
- payment