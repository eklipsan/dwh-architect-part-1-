# Введение в процедуры в PostgreSQL

1.Создать процедуру (ручной вариант coalesce), принимающий 2 аргумента, и возвращающую первый непустой из этих двух аргументов. Функция принимает на вход любые типы данных.


2.Создать процедуру как в первом пункте, но принимающую 3 параметра.

3.Пересоздать процедуру из пункта 2, используя create or replace, добавив в ее тело комментарии, поясняющие логику выполнения.

4.Создать таблицу с одним вещественным полем. Создать процедуру, которая заполняет созданную таблицу случайными вещественными числами от 0 до 1.
Процедура должна принимать на вход одно целое число - количество элементов, которое надо вставить в таблицу.
Процедура должна вернуть среднее значение из всех элементов в таблице.

*5. Создать процедуру, которая будет наполнять таблицу rental новыми записями.
Принимает параметры:
- nm integer - число строк, которое нужно добавить
- dt date default null - дата rental_date, за которую нужно добавить новые записи. Если дата не задана, то находим максимальную существующую дату rental_date в таблице rental и прибавляем к ней один день.
  
Компакт диски для сдачи выбираем случайным образом.