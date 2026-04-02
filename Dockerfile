FROM python:3.11-slim

# Installation de Java (obligatoire pour que PySpark fonctionne)
RUN apt-get update && apt-get install -y default-jre procps && apt-get clean

WORKDIR /app

# On copie et installe les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# On copie tout le reste de ton code
COPY . .

# On expose le port de Streamlit
EXPOSE 8501