# ETL процесс обновления данных в ElasticSearch из БД PostgreSQL.

Для проверки работоспособности достаточно:
- заполнить файлы переменных окружения в /env. Файл .example можно найти там же;

- выполнить build и up docker-compose.yml;
- убедиться в корректном проведении ETL:

загрузка завершается сообщением:
INFO | postgres_extractor:close_connect:62 - Сonnect to postgresql close

а далее идет ожидание из переменной UPDATE_TIME
    
по умолчанию равное 30 секунд;
- запустить проверку тестов с использованием Postman
    (файл ETLTests-2.json).
