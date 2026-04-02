import csv
import json
import time
from kafka import KafkaProducer

# 1. Configuration du Producer
# On augmente la mémoire tampon (buffer) pour tenir la charge des 2 Go
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    batch_size=65536,         # Envoie par paquets de 64Ko pour plus de vitesse
    linger_ms=5,              # Attend 5ms pour grouper les messages
    compression_type='gzip'   # Compresse pour économiser la bande passante
)

topic_name = "clickstream"
csv_file_path = "/home/diabate/Bureau/data/2019-Oct.csv" 

# --- RÉGLAGE DU TEST ---
# On limite à 1 000 000 pour un test de charge solide mais contrôlé
# Mets None si tu veux vraiment envoyer les 2 Go (Attention au disque dur !)
LIMIT = 1000000 

print(f"🔥 DEBUT DU TEST DE CHARGE : Injection en cours...")
start_time = time.time()

try:
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for count, row in enumerate(reader):
            # Vérification de la limite
            if LIMIT and count >= LIMIT:
                print(f"\n🏁 Limite de {LIMIT} atteinte. Fin de l'envoi.")
                break
                
            # Envoi asynchrone
            producer.send(topic_name, value=row)
            
            # Affichage de progression toutes les 100k lignes
            if count % 100000 == 0 and count > 0:
                elapsed = time.time() - start_time
                vitesse = count / elapsed
                print(f"🚀 {count:,} lignes injectées... ({int(vitesse)} msg/s)")

except KeyboardInterrupt:
    print("\n🛑 Arrêt manuel demandé par l'utilisateur.")
except FileNotFoundError:
    print(f"❌ Erreur : Le fichier est introuvable à : {csv_file_path}")
finally:
    # Très important : On attend que tous les messages restants partent
    print("⏳ Vidage du tampon (Flush)...")
    producer.flush()
    producer.close()
    
    total_time = time.time() - start_time
    print(f"🏁 Fin du transfert. Temps total : {total_time:.2f} secondes.")