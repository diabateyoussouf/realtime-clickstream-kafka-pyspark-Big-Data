# Real-Time E-Commerce Clickstream Engine (Kafka + PySpark + Streamlit)

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-2.8+-black?style=for-the-badge&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/PySpark-3.5+-e25a1c?style=for-the-badge&logo=apachespark)
![Streamlit](https://img.shields.io/badge/Streamlit-1.30+-FF4B4B?style=for-the-badge&logo=streamlit)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=for-the-badge&logo=docker)

## À propos du projet
Ce projet est un pipeline de données Big Data en temps réel (Real-Time Data Pipeline) conçu pour ingérer, traiter et analyser des millions d'événements de clics e-commerce (Clickstream). 

L'objectif est d'identifier en direct les abandons de panier et de calculer un Score de Propension (probabilité d'achat) pour chaque utilisateur grâce à un moteur de règles métier sous PySpark. Les résultats sont affichés sur un Dashboard Streamlit orienté prise de décision (Business Intelligence).

Performance démontrée : Ce pipeline est capable d'ingérer et de traiter un flux de plus de 10 000 messages par seconde (Test de charge validé sur un dataset de 2 Go, > 15 millions de lignes).

---

## Architecture Technique

Le flux de données suit une architecture Lambda simplifiée pour le streaming :

1. Ingestion (Producer) : Un script Python lit un dataset massif (ex: données de clics d'octobre 2019) et l'injecte dans un topic Apache Kafka par lots compressés pour un débit maximal.
2. Streaming & Processing (Engine) : PySpark Structured Streaming consomme les messages Kafka en temps réel, applique des micro-batchs, calcule la valeur des paniers et attribue un score de propension (Chaud, Tiède, Froid).
3. Stockage & Checkpointing : Les résultats sont exportés en temps réel dans des fichiers .csv locaux (avec gestion rigoureuse des checkpoints Spark pour la tolérance aux pannes).
4. Visualisation (Dashboard) : Une application Streamlit multi-onglets surveille le dossier d'export et rafraîchit les KPIs métiers (CA en danger, détection de baleines VIP) de manière asynchrone.

---

## Fonctionnalités Principales

* Test de Charge Haute Performance : Injection optimisée avec un KafkaProducer configuré pour le Big Data (batch_size, linger_ms, compression gzip).
* Scoring en Temps Réel : PySpark calcule un score de conversion basé sur le comportement de l'utilisateur (nombre de vues, d'ajouts au panier, interactions).
* Dashboard BI "Live" :
  * Vue Générale : KPIs de chiffre d'affaires récupérable, latence du pipeline, et répartition des clients par segments.
  * Chasse aux VIP (Baleines) : Détection automatique des paniers abandonnés d'une valeur supérieure à 500$ avec un fort score de propension.
  * Analyse Distribution : Histogramme des prix abandonnés pour cibler les efforts marketing.
<img width="1856" height="958" alt="Capture d’écran du 2026-04-02 20-28-46" src="https://github.com/user-attachments/assets/e840c4bd-0dfd-42ed-a67e-8120299813c7" />

---

## Structure du Dépôt

```bash
realtime-clickstream-kafka-pyspark/
│
├── apps/
│   ├── producer.py          # Script d'injection massive des données vers Kafka
│   ├── spark_ml_engine.py   # Moteur PySpark Structured Streaming (Traitement & Scoring)
│   ├── dashboard.py         # Application Streamlit pour le monitoring métier
│
├── docker-compose.yml       # Déploiement de l'infrastructure Kafka & Zookeeper
├── requirements.txt         # Dépendances Python
└── README.md
