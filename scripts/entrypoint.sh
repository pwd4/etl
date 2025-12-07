#!/bin/bash
set -e

# === Fernet key generation ===
if [ "$AIRFLOW__CORE__FERNET_KEY" = "auto" ]; then
  export AIRFLOW__CORE__FERNET_KEY=$(python3 - <<EOF
import base64, os
print(base64.urlsafe_b64encode(os.urandom(32)).decode())
EOF
)
  echo "Generated Fernet Key: $AIRFLOW__CORE__FERNET_KEY"
fi

echo "Initializing Airflow DB..."
airflow db init

# wait DB tables are ready
sleep 2

echo "Creating Airflow user..."
airflow users create \
  --username "$_AIRFLOW_WWW_USER_USERNAME" \
  --password "$_AIRFLOW_WWW_USER_PASSWORD" \
  --firstname "$_AIRFLOW_WWW_USER_FIRSTNAME" \
  --lastname "$_AIRFLOW_WWW_USER_LASTNAME" \
  --email "$_AIRFLOW_WWW_USER_EMAIL" \
  --role Admin || echo "User already exists, skipping..."

echo "Starting Airflow webserver..."
airflow webserver -p 8080 &

echo "Starting Airflow scheduler..."
exec airflow scheduler
