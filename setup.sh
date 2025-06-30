#!/bin/bash
# setup.sh

# Instalar dependencias del sistema para psycopg2
apt-get update
apt-get install -y \
    python3-dev \
    libpq-dev \
    postgresql-client

# Crear y activar el entorno virtual
python -m venv /opt/venv
source /opt/venv/bin/activate

# Instalar dependencias de Python
pip install --upgrade pip
pip install -r requirements.txt