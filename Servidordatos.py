import paho.mqtt.client as mqtt
import pymysql
import sys
import logging
import time
from dotenv import load_dotenv
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT")),
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

MQTT_CONFIG = {
    "host": os.getenv("MQTT_HOST"),
    "port": int(os.getenv("MQTT_PORT")),
    "username": os.getenv("MQTT_USER"),
    "password": os.getenv("MQTT_PASSWORD"),
    "topic": os.getenv("MQTT_TOPIC")
}

# ---------------------
# Conectar a la DB con reintentos
# ---------------------
def connect_db(retries=5, delay=5):
    for attempt in range(1, retries+1):
        try:
            db = pymysql.connect(**DB_CONFIG)
            logging.info("Conectado a la base de datos")
            return db
        except pymysql.MySQLError as e:
            logging.error("Error conectando a DB (intento %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    logging.critical("No se pudo conectar a la base de datos. Saliendo...")
    sys.exit()

db = connect_db()

# ---------------------
# Callbacks MQTT
# ---------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Conectado al broker MQTT")
        client.subscribe(MQTT_CONFIG['topic'])
    else:
        logging.error("Error de conexión MQTT: %s", rc)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logging.warning("Desconectado inesperadamente del broker, reconectando...")
        try:
            client.reconnect()
        except Exception as e:
            logging.error("Fallo al reconectar: %s", e)
            time.sleep(5)

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode('utf-8')
        lista = payload_str.split("&")

        if len(lista) < 8:
            logging.warning("Mensaje incompleto: %s", payload_str)
            return

        sql = """
        INSERT INTO data_reg
        (id_data, codigo_chip, cuenta_in1, cuenta_in2, cuenta_out1, cuenta_out2, crc, evento, estado_relay, Fecha_data, codigo_data_id)
        VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, NULL)
        """
        params = (lista[0], lista[1], lista[2], lista[3], lista[4], lista[5], lista[6], lista[7])

        # Intentar insertar con reintentos
        for attempt in range(3):
            try:
                with db.cursor() as cursor:
                    cursor.execute(sql, params)
                db.commit()
                logging.info("Guardado en DB: %s", payload_str)
                break
            except pymysql.MySQLError as e:
                db.rollback()
                logging.error("Error guardando en DB (intento %d/3): %s", attempt+1, e)
                time.sleep(1)
        else:
            logging.error("No se pudo guardar el mensaje tras 3 intentos: %s", payload_str)

    except Exception as e:
        logging.exception("Error procesando mensaje MQTT: %s", e)

# ---------------------
# Cliente MQTT
# ---------------------
#client = mqtt.Client("Servidor de Datos")
client = mqtt.Client(client_id="Servidor de Datos", protocol=mqtt.MQTTv311)
client.username_pw_set(MQTT_CONFIG['username'], MQTT_CONFIG['password'])
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# ---------------------
# Conectar y loop
# ---------------------
while True:
    try:
        client.connect(MQTT_CONFIG['host'], MQTT_CONFIG['port'], 60)
        client.loop_forever()
    except KeyboardInterrupt:
        logging.info("Cerrando MQTT y DB...")
        client.disconnect()
        db.close()
        sys.exit()
    except Exception as e:
        logging.error("Error en MQTT: %s. Reintentando en 5s...", e)
        time.sleep(5)

    if __name__ == '__main__':
        #app.run(port=6000, debug=True)
        serve (Servidordatos, host='0.0.0.0', port=80)
    