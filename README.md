# Переобучение модели в Airflow

Основа: [код семинара](https://github.com/NikitaMalykhin/mlops_realtime_fraud_detector_airflow). Скоринг читает Kafka, записывает score, fraud_flag и model_version в PostgreSQL.

## Запуск

Нужны Docker Desktop и Compose v2.

Поместите размеченный train.csv в data/train.csv. Подойдут данные прошлых заданий или [данные семинара](https://www.kaggle.com/competitions/teta-ml-1-2025/data). Нужны три последовательных месяца и оба класса в каждом. Столбцы указаны в LABELED_COLUMNS в pipeline/db.py. 

Из корня проекта:

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps -a
```

Другой каталог данных можно задать через TRAIN_DATA_DIR в .env.

[Airflow](http://localhost:8081): admin / admin. Контейнеры data_loader и kafka-setup должны завершиться с кодом 0.

## Решение

Все DAG запускаются каждые 5 минут, catchup=False.

- supply_labeled_batch добавляет следующий месяц: до 10000 строк из витрины с переносом даты. Глубина истории: 36 месяцев.
- retrain_catboost обновляет окно, готовит признаки, обучает и сравнивает модели. Окно: 3 месяца, первые 2 идут в train, последний в test.
- data_quality проверяет последние 3 месяца и сохраняет HTML-отчёт.

Размер батча, история и окно настраиваются в .env. Графы находятся в dags/, функции задач в pipeline/.

Собственные признаки в fraud_detector/src/preprocessing.py: amount_log, hour_sin, hour_cos, is_night, amount_per_capita. Препроцессинг общий для обучения и скоринга.

Кандидат и рабочая модель сравниваются на одном test по ROC-AUC и F1 с порогом score > 0.5. Публикация разрешена, если обе метрики не снизились и хотя бы одна выросла. Решения и метрики сохраняются в training_runs.

Модели доступны Airflow в /opt/airflow/artifacts/models, скорингу в /artifacts/models через общий том. Смена latest.txt обновляет модель без перезапуска.

Батч и отметка о загрузке записываются одной транзакцией. После сохранения кандидата фиксируются окно и обработанный месяц. Повтор завершённого запуска не создаёт дублей и не сдвигает месяц повторно. Отклонение кандидата сохраняет рабочую модель, но месяц считается обработанным.

## Data Quality

1. Все обязательные столбцы присутствуют.
2. Время, сумма, target и категориальные поля заполнены.
3. Время корректно, окно содержит три последовательных месяца.
4. Сумма конечна и неотрицательна.
5. Target принимает 0 и 1, оба класса есть в каждом месяце.

Отчёт сохраняется до выставления статуса в /opt/airflow/artifacts/dq_reports/latest.html, рядом с каталогом моделей.

## Проверка

В Airflow по очереди запустите supply_labeled_batch, retrain_catboost и data_quality через Trigger DAG. Дождитесь завершения каждого.

Проверьте файлы модели, скоринг и решение о публикации:

```bash
docker compose exec airflow cat /opt/airflow/artifacts/models/latest.txt
docker compose exec airflow ls /opt/airflow/artifacts/models
docker compose exec postgres psql -U fraud_user -d fraud_db -c "SELECT score, fraud_flag, model_version, created_at FROM scores ORDER BY id DESC LIMIT 10;"
docker compose exec postgres psql -U fraud_user -d fraud_db -c "SELECT batch_month, version, status FROM training_runs ORDER BY created_at DESC LIMIT 5;"
docker compose exec postgres psql -U fraud_user -d fraud_db -c "TABLE training_state;"
```

```bash
docker compose cp airflow:/opt/airflow/artifacts/dq_reports/latest.html ./dq_report.html
```

Остановка с сохранением данных: `docker compose down`.
