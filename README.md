# Water-Management-Network
<img width="1114" height="1141" alt="image" src="https://github.com/user-attachments/assets/e484ad25-503a-478c-8dbc-7f162f316fa6" />

# Water Management Network - Sistemas Distribuidos UA

Sistema distribuido para la gestión centralizada del riego en parques y jardines. Desarrollado en Java integrando comunicación síncrona mediante Sockets TCP (control de salud y registro) y mensajería asíncrona con Apache Kafka (órdenes de riego y telemetría de consumo de agua en tiempo real).

## Requisitos Previos
* **Java Development Kit (JDK 17+)**
* **Docker y Docker Desktop** (en ejecución)
* Librerías externas en la raíz del proyecto:
  * `kafka-clients.jar`
  * `slf4j-api.jar`
  * `slf4j-simple.jar`

---

## 🐳 Gestión del Entorno Docker (Kafka)

Antes de ejecutar los programas en Java, es necesario levantar el broker de Apache Kafka.

**Levantar la infraestructura (Modo *detached*):**
```bash
docker compose up -d
```

**Apagar la infraestructura (Obligatorio al terminar):**
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

**(Windows - PowerShell / CMD):**
```powershell
javac -cp ".;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" ProtocoloUtil.java WM_WS_M.java WM_WS_E.java
```
**(Linux / Mac):**
```bash
javac -cp ".:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar" ProtocoloUtil.java WM_WS_M.java WM_WS_E.java
```

---

## 🚀 Ejecución de los Módulos (Estación de Riego)

Cada módulo debe ejecutarse en una terminal independiente.

**1. Levantar el Monitor (`WM_WS_M`):**
> Sintaxis: `java WM_WS_M <Puerto_Escucha_Engine> <IP_Central> <Puerto_Central> <ID_WS>`
```powershell
java WM_WS_M 5000 localhost 8080 WS_01
```

**2. Levantar el Engine (`WM_WS_E`):**
> Sintaxis: `java -cp "..." WM_WS_E <IP_Kafka> <Puerto_Kafka> <IP_Monitor> <Puerto_Monitor> [ID_WS]`
```powershell
java -cp ".;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_WS_E localhost 9092 localhost 5000 WS_01
```
*Nota: Pulsa `ENTER` en la terminal del Engine en cualquier momento para simular una avería (KO) y comprobar el reporte automático de fugas al Monitor.*

---

## 🧪 Guía de Pruebas de Extremo a Extremo (4 Terminales)

Sigue estos pasos para verificar el flujo de órdenes y la emisión continua de telemetría:

### Terminal 1: Monitor (`WM_WS_M`)
```powershell
java WM_WS_M 5000 localhost 8080 WS_01
```

### Terminal 2: Engine (`WM_WS_E`)
```powershell
java -cp ".;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_WS_E localhost 9092 localhost 5000 WS_01
```

### Terminal 3: Consumidor de Telemetría (Kafka)
Escucha en tiempo real la telemetría (caudal y volumen acumulado) emitida por la estación:
```powershell
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic telemetria-estaciones
```

### Terminal 4: Productor de Órdenes (Kafka)
Envía comandos para iniciar y detener el riego:

- **Iniciar Riego:**
  ```powershell
  "INICIAR_RIEGO#WS_01" | docker exec -i broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ordenes-riego
  ```
  *(Observa cómo la Terminal 2 y la Terminal 3 comienzan a recibir y mostrar tramas de telemetría cada segundo).*

- **Detener Riego:**
  ```powershell
  "PARAR_RIEGO#WS_01" | docker exec -i broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ordenes-riego
  ```
  *(La estación detiene la electroválvula y finaliza el envío de telemetría).*

---

## 📋 Estructura de Tramas y Mensajes

- **Sockets TCP (Fugas / Registro)**: Formato ASCII delimitado por `<STX>` y `<ETX>` con verificación `LRC` (`ProtocoloUtil.java`).
  - Ejemplo: `REGISTRO#WS_01#Parque Central` | `FUGA#WS_01`
- **Kafka - Topic `telemetria-estaciones`**:
  - Formato: `TELEMETRIA#<ID_WS>#<CAUDAL>#<VOLUMEN_ACUMULADO>#<TIMESTAMP>`
  - Ejemplo: `TELEMETRIA#WS_01#12.50#37.50#1790584334749`