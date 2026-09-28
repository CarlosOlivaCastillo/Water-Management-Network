# Water-Management-Network
<img width="1114" height="1141" alt="image" src="https://github.com/user-attachments/assets/e484ad25-503a-478c-8dbc-7f162f316fa6" />

# Water Management Network - Sistemas Distribuidos UA

Sistema distribuido para la gestión centralizada del riego en parques y jardines. Desarrollado en Java integrando comunicación síncrona mediante Sockets TCP (control de salud y registro) y mensajería asíncrona con Apache Kafka (órdenes de riego y telemetría).

## Requisitos Previos
* **Java Development Kit (JDK)**
* **Docker y Docker Desktop** (en ejecución)
* Librerías externas en la raíz del proyecto:
  * `kafka-clients.jar`
  * `slf4j-api.jar`
  * `slf4j-simple.jar`

---

## 🐳 Gestión del Entorno Docker (Kafka)

Antes de ejecutar los programas en Java, es necesario levantar el gestor de colas.

**Levantar la infraestructura (Modo *detached*):**
```bash
docker compose up -d
```

**Acostar / Apagar la infraestructura (Obligatorio al terminar):**
```bash
docker compose down
```

**Forzar limpieza si el contenedor se queda bloqueado o da error de conflicto:**
```bash
docker rm -f broker
```

---

## 🛠️ Compilación del Proyecto

Para compilar las clases del Monitor, Engine y el Protocolo, incluyendo las librerías externas de Kafka y SLF4J en el Classpath:

**(Windows - PowerShell):**
```powershell
javac -cp ".;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_WS_E.java WM_WS_M.java ProtocoloUtil.java
```
**(Linux / Mac):**
```bash
javac -cp ".:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar" WM_WS_E.java WM_WS_M.java ProtocoloUtil.java
```

---

## 🚀 Ejecución de los Módulos (Estación de Riego)

Cada módulo debe ejecutarse en una terminal independiente.

**1. Levantar el Monitor (`WM_WS_M`):**
> Sintaxis: java WM_WS_M <Puerto_Local> <IP_Central> <Puerto_Central> <ID_WS>
```bash
java WM_WS_M 5000 localhost 8080 WS-01
```

**2. Levantar el Engine (`WM_WS_E`):**
> Sintaxis: java -cp "..." WM_WS_E <IP_Kafka> <Puerto_Kafka> <IP_Monitor> <Puerto_Monitor>
```bash
java -cp ".;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_WS_E localhost 9092 localhost 5000
```
*Nota: Pulsa `ENTER` en la terminal del Engine en cualquier momento para simular una avería (KO) y comprobar el reporte automático de fugas.*

---

## 🧪 Pruebas Manuales con Kafka (Terminales Docker)

Comandos útiles para interactuar con los *topics* directamente desde el contenedor sin usar el código de Java.

**Escuchar mensajes como Consumidor:**
```bash
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ordenes-riego --from-beginning
```

**Enviar mensajes como Productor:**
```bash
docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ordenes-riego
```
> *Ejemplo de mensaje a enviar: `INICIAR_RIEGO`*
