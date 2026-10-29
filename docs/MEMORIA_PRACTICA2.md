# Memoria de Prácticas - Sistemas Distribuidos (Curso 2026/2027)

---

## Portada

* **Título de la Práctica:** Práctica 2 (Release 1) – *Water Management Network: Sockets, Streaming de Eventos, Colas y Modularidad*
* **Asignatura:** Sistemas Distribuidos (Grado en Ingeniería Informática - Universidad de Alicante)
* **Curso Académico:** 2026 / 2027
* **Integrantes del Grupo:**
  * **Estudiante 1:** Carlos Oliva Castillo — **DNI:** `50384072L`
  * **Estudiante 2:** Nael Alba — **DNI:** `48804733K`
* **Repositorio Oficial en GitHub:** `https://github.com/CarlosOlivaCastillo/Water-Management-Network`
* **Imágenes en Docker Hub (CI/CD):** `carlosolivaa/practica-aws:latest` y `carlosolivaa/wm-central:latest`

---

## Índice

1. [Resumen Ejecutivo y Arquitectura del Sistema](#1-resumen-ejecutivo-y-arquitectura-del-sistema)
2. [Descripción Detallada de los Componentes Software Desarrollados](#2-descripción-detallada-de-los-componentes-software-desarrollados)
   * 2.1. `ProtocoloUtil.java` (Empaquetado `<STX><DATA><ETX><LRC>`)
   * 2.2. `WM_Central.java` (Gobierno Central, Sockets y Eventos Kafka)
   * 2.3. `GestorCentral.java` (Patrón Singleton y Persistencia SQLite)
   * 2.4. `HiloServidor.java` (Atención Concurrente a Monitores TCP)
   * 2.5. `ServidorWebCentral.java` (Dashboard Web en Tiempo Real y API REST)
   * 2.6. `WM_WS_M.java` (Monitor de Estación de Riego)
   * 2.7. `WM_WS_E.java` (Engine de Estación de Riego: Electroválvula y Caudalímetro)
   * 2.8. `WM_FO.java` (Aplicación de Operarios de Campo - Field Operators)
3. [Protocolos de Comunicación y Diseño de Mensajería](#3-protocolos-de-comunicación-y-diseño-de-mensajería)
4. [Guía Paso a Paso de Despliegue (Cloud AWS + Laboratorio Docker)](#4-guía-paso-a-paso-de-despliegue-cloud-aws--laboratorio-docker)
5. [Guion y Capturas de Pantalla del Funcionamiento](#5-guion-y-capturas-de-pantalla-del-funcionamiento)

---

## 1. Resumen Ejecutivo y Arquitectura del Sistema

**Water Management Network** es un sistema distribuido en tiempo real diseñado para la supervisión y gestión centralizada de la red de estaciones de riego (*Watering Stations* - `WS`) distribuidas por los parques y jardines de una ciudad.

La arquitectura combina de manera híbrida dos paradigmas de comunicación fundamentales en sistemas distribuidos:
1. **Comunicación Síncrona orientada a conexión mediante Sockets TCP (`java.net.ServerSocket` / `Socket`)**: Utilizada para tareas críticas de control de salud (*heartbeat* cada segundo entre Monitor y Engine), autenticación/registro inicial de estaciones ante la Central y notificación inmediata de averías/fugas (`FUGA`) y su resolución (`FIN_FUGA`), garantizando la integridad de cada trama mediante empaquetado `<STX><DATA><ETX><LRC>`.
2. **Comunicación Asíncrona basada en Eventos y Colas mediante Apache Kafka**: Empleada para desacoplar las solicitudes de los operarios de campo (`WM_FO`), la emisión de órdenes de apertura/cierre de electroválvulas (`ordenes-riego`) y el *streaming* continuo de telemetría de consumo de agua cada segundo (`telemetria-estaciones`).

```
+-----------------------------------------------------------------------------------+
|                                CLOUD HOSTING (AWS EC2)                            |
|                                                                                   |
|  +-----------------------+       JDBC        +---------------------------------+  |
|  |   SQLite Database     | <---------------> |           WM_Central            |  |
|  | (watermanagement.db)  |                   |  - GestorCentral (Singleton)    |  |
|  | - Tabla: estaciones   |                   |  - HiloServidor (1..N clientes) |  |
|  | - Tabla: operarios    |                   |  - ServidorWebCentral (:8081)   |  |
|  +-----------------------+                   +--------+---------------+--------+  |
|                                                       |               ^           |
|                                                       | Kafka         | Sockets   |
|                                                       v               | TCP :8080 |
|                                              +--------+---------------+--------+  |
|                                              |     Apache Kafka Broker (:9092) |  |
|                                              |  - peticiones_riego             |  |
|                                              |  - respuestas_riego             |  |
|                                              |  - ordenes-riego                |  |
|                                              |  - telemetria-estaciones        |  |
|                                              +--------+---------------+--------+  |
+-------------------------------------------------------|---------------|-----------+
                                                        |               |
                     +----------------------------------+               |
                     | Kafka (:9092)                                    | Sockets TCP (:8080)
                     v                                                  | y Kafka (:9092)
+--------------------+--------------------+       +---------------------+--------------------+
|               PC LAB 1                  |       |                 PC LAB 2                 |
|                                         |       |                                          |
|  +-----------------------------------+  |       |  +------------------------------------+  |
|  |       WM_FO (1) - Operario FO_01  |  |       |  |     Watering Station 1 (WS_01)     |  |
|  |  - Modo Interactivo / Lotes (.txt)|  |       |  |  +--------------+  Sockets  +----+ |  |
|  +-----------------------------------+  |       |  |  | WM_WS_M (:5001) <------> | E  | |  |
|  +-----------------------------------+  |       |  |  +--------------+   1 seg   +----+ |  |
|  |       WM_FO (2) - Operario FO_02  |  |       |  +------------------------------------+  |
|  |  - Espera de 4s entre servicios   |  |       |  |     Watering Station 2 (WS_02)     |  |
|  +-----------------------------------+  |       |  +------------------------------------+  |
+-----------------------------------------+       +------------------------------------------+
```

---

## 2. Descripción Detallada de los Componentes Software Desarrollados

### 2.1. `ProtocoloUtil.java` (Empaquetado y Verificación de Integridad `<STX><DATA><ETX><LRC>`)
Clase de utilidad transversal encargada de encapsular y validar las tramas transmitidas por Sockets TCP entre `WM_WS_E`, `WM_WS_M` y `WM_Central`:
* **`empaquetar(String mensaje)`**: Calcula el byte de redundancia longitudinal (`LRC`) aplicando la operación lógica `XOR` carácter a carácter sobre el cuerpo del mensaje, devolviendo la secuencia `<STX (0x02)><MENSAJE><ETX (0x03)><LRC>`.
* **`desempaquetar(String trama)`**: Verifica la presencia de los delimitadores `STX` y `ETX`, extrae la carga útil y recalcula el `LRC`. Si difiere del recibido, lanza una excepción que provoca el envío inmediato de `<NACK>`.

### 2.2. `WM_Central.java` (Módulo Central de Gobierno)
Aplicación principal del *Core System*. Recibe por línea de comandos:
`java WM_Central <puerto_central> <ip_kafka> <puerto_kafka> [puerto_web]`
* Inicializa la base de datos SQLite poniendo todas las estaciones registradas en estado `DESCONECTADA` hasta que sus respectivos monitores conecten.
* Levanta el `ServerSocket` multihilo en el puerto `8080` creando una instancia de `HiloServidor` por cada estación que se conecta.
* Gestiona un `KafkaProducer` y dos `KafkaConsumer` concurrentes:
  * Consumidor del topic `peticiones_riego`: valida disponibilidad y operario en BD, publica `INICIAR_RIEGO#<idWS>` en `ordenes-riego`, notifica `ACK#<idWS>` o `NACK#<idWS>` al operario en `respuestas_riego` y programa un temporizador en un `ScheduledExecutorService` para cortar el suministro automáticamente al cumplirse el tiempo establecido y enviar el resumen `FIN_RIEGO#<idWS>#<litros>`.
  * Consumidor del topic `telemetria-estaciones`: actualiza en tiempo real el caudal y el volumen acumulado tanto en consola como en el Dashboard Web.
* Ofrece control arbitrario (Punto 11 de la mecánica) tanto por consola interactiva como desde el Dashboard Web para **Iniciar un riego**, **Bloquear una WS (`FUERA_DE_SERVICIO`)** y **Activar una WS (`DISPONIBLE`)**.

### 2.3. `GestorCentral.java` (Singleton y Acceso a Datos SQLite)
Implementa el patrón de diseño **Singleton** con métodos `synchronized` para garantizar la seguridad ante accesos concurrentes (*thread-safety*) desde múltiples hilos de sockets y consumidores Kafka sobre la base de datos `watermanagement.db` (SQLite):
* **Tabla `estaciones`**: Almacena `idWS`, `ubicacion`, `estado` (`DISPONIBLE`, `REGANDO`, `FUGA`, `FUERA_DE_SERVICIO`, `DESCONECTADA`), `idFO`, `duracion` y `fecha_inicio`. Precarga las 7 estaciones oficiales de la red municipal (`WS_01` a `WS_07`).
* **Tabla `operarios`**: Almacena los identificadores dados de alta en Central (`FO_01`, `FO_02`, `FO_03`, `CENTRAL`, `WEB_PANEL`) validados mediante `validarOperario(idFO)`.

### 2.4. `HiloServidor.java` (Gestión Concurrente de Conexiones de Monitores)
Hilo dedicado a cada conexión entrante en el servidor de sockets de Central:
* Desempaqueta las tramas `<STX><DATA><ETX><LRC>` y delega la operación en `GestorCentral.procesarPeticion(...)`.
* Si recibe `FUGA#<idWS>` mientras la estación se encontraba en estado `REGANDO` (indicado por el sufijo `#R`), invoca `WM_Central.cortarAguaFuga(idWS)`, cerrando de inmediato la electroválvula por Kafka, cancelando el temporizador activo y alertando al operario (`FUGA#<idWS>`).
* Si recibe `FIN_FUGA#<idWS>`, restaura la estación al estado `DISPONIBLE`.
* En su bloque `finally`, si el socket del Monitor se cierra o cae la red, actualiza automáticamente el estado de esa estación a `DESCONECTADA`.

### 2.5. `ServidorWebCentral.java` (Cuadro de Mandos Web en Tiempo Real)
Servidor HTTP embebido (`com.sun.net.httpserver.HttpServer`) que escucha en el puerto `8081` y sirve el panel de monitorización visual junto con una API REST JSON (`/api/estaciones`, `/api/telemetria`, `/api/logs`, `/api/riego`, `/api/estado`, `/api/emergencia`).
Cumple estrictamente los colores oficiales del enunciado (página 4):
* **Disponible (`DISPONIBLE`)**: Fondo **VERDE**.
* **Regando (`REGANDO`)**: Fondo **VERDE PARPADEANDO** mostrando en tiempo real el caudal en L/min, el volumen acumulado en litros y el ID del operario que activó el riego.
* **Fuga (`FUGA`)**: Fondo **ROJO**.
* **Fuera de Servicio (`FUERA_DE_SERVICIO`)**: Fondo **NARANJA** con la leyenda *"Fuera de Servicio"*.
* **Desconectada (`DESCONECTADA`)**: Fondo **GRIS**.

### 2.6. `WM_WS_M.java` (Monitor de la Estación de Riego)
Recibe por línea de parámetros:
`java WM_WS_M <Puerto_Escucha_Engine> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]`
* Al arrancar se conecta por Sockets TCP a `WM_Central` (con política de reintentos para resiliencia) y envía la trama `REGISTRO#<idWS>#<ubicacion>`.
* Abre un `ServerSocket` local donde acepta la conexión de su `WM_WS_E` y ejecuta un bucle de comprobación de salud cada **1 segundo** enviando `STATUS_CHECK`.
* Si recibe `KO` o pierde la conexión con el Engine, envía inmediatamente `FUGA#<idWS>` a Central. Cuando la contingencia se resuelve en el Engine y vuelve a recibir `OK`, envía `FIN_FUGA#<idWS>` a Central para devolver la estación a `DISPONIBLE`.

### 2.7. `WM_WS_E.java` (Engine de la Estación de Riego)
Recibe por línea de parámetros:
`java WM_WS_E <IP_Kafka> <Puerto_Kafka> <IP_Monitor> <Puerto_Monitor> [ID_WS]`
* Se suscribe al topic de Kafka `ordenes-riego` filtrando los mensajes dirigidos a su `ID_WS` (o `ALL`) para abrir (`INICIAR_RIEGO`) o cerrar (`PARAR_RIEGO`) la electroválvula.
* Mientras `regando == true`, su hilo productor envía cada **1 segundo** al topic `telemetria-estaciones` la trama `TELEMETRIA#<idWS>#<caudal>#<volumenAcumulado>#<timestamp>`.
* Incluye un hilo interactivo de teclado que permite:
  1. Pulsar `ENTER` (o `1`) para simular una fuga (`KO`) en tiempo de ejecución o volver a pulsar `ENTER` para resolver la avería (`OK`).
  2. Iniciar (`2` / `RIEGO`) o detener (`3` / `PARAR`) el suministro manualmente desde la propia estación.

### 2.8. `WM_FO.java` (Aplicación de Operarios de Campo - Field Operators)
Recibe por línea de parámetros:
`java WM_FO <IP_Kafka> <Puerto_Kafka> <ID_FO> [Ruta_Fichero | <ID_WS> <Duracion>]`
* Envía peticiones `START_RIEGO#<idWS>#<idFO>#<duracion>` y `STOP_RIEGO#<idWS>#<idFO>` al topic `peticiones_riego` de Kafka.
* Escucha de forma asíncrona en el topic `respuestas_riego` filtrando por clave `idFO` para mostrar en pantalla la autorización (`ACK`), denegación (`NACK`), corte por avería (`FUGA`) o el resumen final del riego con los litros consumidos (`FIN_RIEGO#<idWS>#<litros>`).
* Al procesar un fichero de servicios por lotes (`tareas.txt`), agrupa las órdenes separadas por `;` y las envía a `WM_Central`, quien las encola por operario (`colasFO`), ejecuta cada servicio hasta su conclusión (en éxito, denegación o corte por fuga) y **espera reglamentariamente 4 segundos** (`espera(idFO)`) antes de despachar el siguiente servicio de la cola.

---

## 3. Protocolos de Comunicación y Diseño de Mensajería

### 3.1. Tramas Síncronas por Sockets TCP (`<STX><DATA><ETX><LRC>`)

| Origen -> Destino | Mensaje (`<DATA>`) | Respuesta | Descripción |
| :--- | :--- | :--- | :--- |
| `WM_WS_M` -> `WM_Central` | `REGISTRO#<idWS>#<ubicacion>` | `<ACK>` / `<NACK>` | Alta y conexión de la estación (`DISPONIBLE`). |
| `WM_WS_M` -> `WM_Central` | `FUGA#<idWS>` | `<ACK>` / `<NACK>` | Notificación inmediata de fuga (`FUGA`) y corte de riego si estaba activo. |
| `WM_WS_M` -> `WM_Central` | `FIN_FUGA#<idWS>` | `<ACK>` / `<NACK>` | Contingencia resuelta; la estación vuelve a `DISPONIBLE`. |
| `WM_WS_M` -> `WM_WS_E` | `STATUS_CHECK` | `OK` / `KO` | Comprobación de salud (*heartbeat*) cada 1 segundo. |

### 3.2. Eventos Asíncronos en Apache Kafka

| Topic de Kafka | Productor | Consumidor | Formato del Mensaje (`Value`) |
| :--- | :--- | :--- | :--- |
| `peticiones_riego` | `WM_FO`, `WM_WS_E` | `WM_Central` | `START_RIEGO#<idWS>#<idFO>#<duracion>` \| `STOP_RIEGO#<idWS>#<idFO>` |
| `respuestas_riego` | `WM_Central` | `WM_FO` | `ACK#<idWS>` \| `NACK#<idWS>` \| `FUGA#<idWS>` \| `FIN_RIEGO#<idWS>#<litros>` |
| `ordenes-riego` | `WM_Central` | `WM_WS_E` | `INICIAR_RIEGO#<idWS>` \| `PARAR_RIEGO#<idWS>` \| `PARAR_RIEGO#ALL` |
| `telemetria-estaciones` | `WM_WS_E` | `WM_Central` | `TELEMETRIA#<idWS>#<caudal>#<volumenAcumulado>#<timestamp>` |

---

## 4. Guía Paso a Paso de Despliegue (Cloud AWS + Laboratorio Docker)

Esta es la guía exacta utilizada para desplegar el escenario físico distribuido (Figura 4 del enunciado):

### Paso 1: Tubería CI/CD Automática (VS Code -> GitHub Actions -> Docker Hub)
1. Cada `git push` a la rama `main` o `develop` dispara el workflow `.github/workflows/deploy.yml`.
2. GitHub Actions compila la imagen multi-etapa definida en el `Dockerfile` raíz y la publica automáticamente en Docker Hub bajo `carlosolivaa/practica-aws:latest` y `carlosolivaa/wm-central:latest`.

### Paso 2: Despliegue del Entorno Cloud en AWS EC2 (`WM_Central` + `SQLite` + `Kafka`)
1. En **AWS Academy Learner Lab** (`us-east-1`), lanzar una instancia **EC2 Ubuntu (`t3.micro`)** con el par de claves `vockey` y el grupo de seguridad `default` con los puertos de entrada **22 (SSH)**, **9092 (Kafka)**, **8080 (Sockets Central)** y **8081 (Dashboard Web)** abiertos a `0.0.0.0/0`.
2. Conectarse por SSH a la instancia EC2 y ejecutar los contenedores Cloud:
   ```bash
   sudo apt-get update -y && sudo apt-get install -y docker.io
   sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
   PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)

   # 1. Levantar Broker Apache Kafka anunciando la IP pública de AWS
   sudo docker run -d --name kafka --restart unless-stopped -p 9092:9092 \
     -e KAFKA_HEAP_OPTS="-Xmx350M -Xms350M" \
     -e KAFKA_CFG_NODE_ID=0 \
     -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
     -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
     -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
     -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9093 \
     -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
     -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://$PUBLIC_IP:9092 \
     bitnamilegacy/kafka:3.7.0

   # 2. Levantar WM_Central + SQLite + Dashboard Web desde Docker Hub
   sudo docker run -d --name wm_central --restart unless-stopped -p 8080:8080 -p 8081:8081 \
     -e PUERTO_CENTRAL=8080 \
     -e IP_KAFKA=$PUBLIC_IP \
     -e PUERTO_KAFKA=9092 \
     -e PORT=8081 \
     carlosolivaa/practica-aws:latest
   ```
3. Verificar el **Dashboard Web en la nube**: `http://<IP_PUBLICA_AWS>:8081` (ej. `http://100.53.209.11:8081`).

### Paso 3: Despliegue en PC LAB 2 — Estaciones de Riego (`WM_WS 1` y `WM_WS 2`)
En el ordenador del Laboratorio 2, abrir PowerShell en la carpeta del repositorio y apuntar a la IP pública de AWS:
```powershell
$env:IP_CENTRAL="100.53.209.11"
$env:PUERTO_CENTRAL="8080"
$env:IP_KAFKA="100.53.209.11"
$env:PUERTO_KAFKA="9092"

docker compose -f docker-compose.lab-ws.yml up -d --build
```
* Para interactuar con el menú de la estación `WS_01` (simular fuga `KO` pulsando `ENTER`, resolverla volviendo a pulsar `ENTER`, o iniciar/parar riego manual con `2`/`3`):
  ```powershell
  docker attach ws01_engine
  ```

### Paso 4: Despliegue en PC LAB 1 — Operarios de Campo (`WM_FO 1` y `WM_FO 2`)
En el ordenador del Laboratorio 1, abrir dos terminales de PowerShell apuntando al Kafka de AWS:

* **Terminal 1 (`WM_FO 1` - Operario `FO_01` en modo interactivo o por fichero `tareas.txt`):**
  ```powershell
  $env:IP_KAFKA="100.53.209.11"; $env:PUERTO_KAFKA="9092"
  docker compose -f docker-compose.lab-fo.yml run --rm fo_01
  ```
  *(Dentro de la consola de `FO_01`, escribir `WS_01#10` para regar 10 segundos o `FILE#tareas.txt` para ejecutar el lote automático con espera de 4 segundos entre servicios).*

* **Terminal 2 (`WM_FO 2` - Operario `FO_02`):**
  ```powershell
  $env:IP_KAFKA="100.53.209.11"; $env:PUERTO_KAFKA="9092"
  docker compose -f docker-compose.lab-fo.yml run --rm fo_02
  ```

---

## 5. Guion y Capturas de Pantalla del Funcionamiento

1. **Captura 1 – Pipeline CI/CD en GitHub Actions y Docker Hub:**
   * Muestra la ejecución con tic verde ✅ del workflow *Build and Push to Docker Hub* tras hacer `git push` y el repositorio `carlosolivaa/practica-aws` actualizado en Docker Hub.
2. **Captura 2 – Instancia AWS EC2 y Contenedores Cloud (`wm_central` y `kafka`) en ejecución:**
   * Muestra el panel de AWS EC2 (`Cloud-WM` con IP pública `100.53.209.11`) y la salida de `sudo docker ps` con `wm_central` y `kafka` activos.
3. **Captura 3 – Cuadro de Mandos Web de `WM_Central` con Estaciones Conectadas (Estado VERDE `DISPONIBLE` y GRIS `DESCONECTADA`):**
   * Muestra `http://100.53.209.11:8081` tras levantar `docker-compose.lab-ws.yml`, donde `WS_01` y `WS_02` pasan de Gris (`DESCONECTADA`) a Verde (`DISPONIBLE`).
4. **Captura 4 – Activación de Riego por `WM_FO` (Individual y por Lotes `tareas.txt`) y Telemetría en Tiempo Real (VERDE PARPADEANDO):**
   * Muestra la terminal de `FO_01` enviando `WS_01#10` (y `FILE#tareas.txt` con la pausa de 4 segundos), el gráfico de caudal en tiempo real, el volumen acumulado en litros y el resumen final `FIN_RIEGO` recibido por el operario.
5. **Captura 5 – Simulación de Fuga (`KO`), Corte Automático de Riego (Estado ROJO `FUGA`) y Bloqueo por Central (Estado NARANJA `FUERA DE SERVICIO`):**
   * Muestra cómo al pulsar `ENTER` en `ws01_engine` el estado pasa inmediatamente a Rojo (`FUGA`), se corta el riego en curso informando a todos los módulos, y cómo al pulsar de nuevo `ENTER` se resuelve la contingencia volviendo a `DISPONIBLE`, además del bloqueo manual desde Central en color Naranja (`FUERA DE SERVICIO`).
