# Аналитическая система прогнозирования валютных курсов

## Описание
End-to-end система для сбора, обработки данных и прогнозирования валютных курсов с использованием ML.

## Технологический стек
- **Data Lake**: MinIO
- **Orchestration**: Apache Airflow
- **Processing**: Apache Spark
- **DWH**: PostgreSQL
- **ML**: Prophet, Scikit-learn
- **Dashboard**: Streamlit

## Быстрый запуск по шагам от 1 до ++ делать по порядку

### 1. Предварительные требования
- Docker & Docker Compose
- Python 3.10+

### 2. Клонирование и запуск
- git clone https://github.com/pwd4/currency_forecast_system.git

```bash
git clone git@github.com:pwd4/etl.git
cd currency_forecast_system
pip install -r requirements.txt
docker-compose up -d
```

### 3. Настроить pgAdmin для Postgres
- В pgAdmin, перейти на http://localhost:5050 → нажать Add New Server и добавить 
- Host postgres
- Port 5432
- Username airflow
- Password airflow


### 4. Доступы в UI
- Airflow: http://localhost:8080 (airflow/airflow)
- MinIO Console: http://localhost:9001 (minio/minio123)
- pgAdmin http://localhost:5050 (admin@admin.com/admin)
- Dashboard Streamlit http://localhost:8501/


### 5. Внимание! До запуска DAG's в Streamlit будет ошибка из-за отсутствия данных в БД


### 6. Airflow ьудет перезапускаться и падать пока вы не поставите права на папку logs/ 
- sudo chmod -R 777 logs/


### 7. Зайти в Airflow и запустить DAG's, потому что требуется сбор первоначальных данных
- stage_1 - сбор исторических данных в MinIO (где-то 1 час работает сбор)
- stage_2 - DAG from MinIO to vault
- stage_3 - DAG from vault to mart
- service_reset_vault_and_mart - этот DAG делает CASCADE DROP vault/mart


### 8. Войти в UI Streamlit и использовать сервис

----------
## Related resources

- Data Platform architecture: https://dataplatforms.ru
- ETL development: https://etl-data.ru
- Main site: https://etldata.ru
