# 💧 Water Management Network — Sistemas Distribuidos (UA 2026/2027)

<img width="1114" height="1141" alt="Arquitectura Water Management Network" src="https://github.com/user-attachments/assets/e484ad25-503a-478c-8dbc-7f162f316fa6" />

Sistema distribuido para la supervisión y gestión centralizada del riego en parques y jardines de una ciudad. Combina **Sockets TCP síncronos con protocolo `<STX><DATA><ETX><LRC>`** (registro de estaciones, *heartbeat* cada 1s y alerta inmediata de fugas) con **mensajería asíncrona en Apache Kafka** (peticiones de operarios, órdenes a electroválvulas y *streaming* de telemetría de caudal/litros en tiempo real) y **persistencia SQLite** con **Cuadro de Mandos Web en tiempo real**.

* **Autores:** Carlos Oliva Castillo (`50384072L`) y Nael Alba (`48804733K`)
* **Imágenes Oficiales Docker Hub (CI/CD):** `carlosolivaa/practica-aws:latest` / `carlosolivaa/wm-central:latest`
* **Documentación de Entrega:**
  * 📄 [Memoria Técnica Completa (`docs/MEMORIA_PRACTICA2.md`)](docs/MEMORIA_PRACTICA2.md)
  * 🤖 [Registro de Prompts de IA (`docs/PROMPTS_IA.md`)](docs/PROMPTS_IA.md)

---

## 🗺️ Mapa Rápido de Puertos e IPs

Cuando despliegues el sistema en **3 ordenadores distintos** (o **AWS Cloud + 2 ordenadores de laboratorio**), **solo necesitas saber UNA IP**: la **IP del Ordenador 1 (o de AWS)** donde corren `WM_Central` y `Apache Kafka`.

| Componente | Dónde se ejecuta | Puertos que escucha | A qué IP se conecta |
| :--- | :--- | :--- | :--- |
| **`WM_Central` + `SQLite` + `Kafka`** | **Ordenador 1** *(o instancia AWS EC2)* | **`8080`** (Sockets TCP)<br>**`8081`** (Dashboard Web HTTP)<br>**`9092`** (Broker Kafka) | Escucha conexiones entrantes (`0.0.0.0`). Anuncia su propia IP (`<IP_CENTRAL>`) en Kafka. |
| **Estaciones (`WM_WS_M` + `WM_WS_E`)** | **Ordenador 2** *(PC Lab Estaciones)* | `5001` (`WS_01`), `5002` (`WS_02`)... *(comunicación interna M <-> E)* | Se conecta a **`<IP_CENTRAL>:8080`** (Sockets) y **`<IP_CENTRAL>:9092`** (Kafka). |
| **Operarios (`WM_FO 1` y `WM_FO 2`)** | **Ordenador 3** *(PC Lab Operarios)* | Ninguno *(cliente Kafka)* | Se conecta a **`<IP_CENTRAL>:9092`** (Kafka). |

---

## 🚀 ESCENARIO 1: Probar con 3 Ordenadores en el Laboratorio (LAN)

Si estás en el laboratorio con **3 ordenadores en la misma red** (o en 2 ordenadores + AWS), tienes **scripts interactivos automáticos** y los **comandos manuales equivalentes**.

---

### 🖥️ ORDENADOR 1: Servidor Central + Kafka + Base de Datos + Web (`:8081`)

#### Opción 1A — Con Script Automático (Recomendado):
Abre PowerShell en la carpeta del proyecto y ejecuta:
```powershell
powershell -ExecutionPolicy Bypass -File .\lab_pc1_central.ps1
```
*(El script detecta automáticamente la IP de red de ese ordenador, te la muestra en grande para que la apuntes, y te deja elegir entre arrancar con Docker Compose o con Java).*

#### Opción 1B — Con Comandos Manuales (Docker Compose):
1. Averigua la IP de red del Ordenador 1 ejecutando `ipconfig` (por ejemplo, `172.20.43.100`).
2. Ejecuta:
   ```powershell
   $env:IP_PUBLICA_HOST="172.20.43.100"   # <-- Pon aquí la IP del Ordenador 1
   docker compose -f docker-compose.cloud.yml up -d --build
   ```
3. Abre el **Dashboard Web** en cualquier navegador: **`http://172.20.43.100:8081`**

#### Opción 1C — Si el Ordenador 1 es tu Instancia Cloud en AWS EC2 (`Cloud-WM`):
1. Enciende la instancia **`Cloud-WM`** en AWS Academy (`us-east-1`) y entra por SSH desde PowerShell:
   ```powershell
   ssh -i "C:\Users\carlo\Downloads\labsuser.pem" ubuntu@<IP_PUBLICA_AWS>
   ```
2. Pega este bloque dentro de la consola `ubuntu@...` de AWS:
   ```bash
   sudo swapon /swapfile 2>/dev/null || true
   PUBLIC_IP=$(curl -s http://checkip.amazonaws.com | tr -d '[:space:]')
   sudo docker rm -f kafka wm_central 2>/dev/null || true
   sudo docker pull carlosolivaa/practica-aws:latest

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

   sudo docker run -d --name wm_central --restart unless-stopped -p 8080:8080 -p 8081:8081 \
     -e PUERTO_CENTRAL=8080 \
     -e IP_KAFKA=$PUBLIC_IP \
     -e PUERTO_KAFKA=9092 \
     -e PORT=8081 \
     carlosolivaa/practica-aws:latest

   echo "Dashboard Web listo en: http://$PUBLIC_IP:8081"
   ```

---

### 🌱 ORDENADOR 2: Estaciones de Riego (`WS_01` y `WS_02`)

Sustituye `<IP_CENTRAL>` por la IP del Ordenador 1 (o la IP pública de AWS).

#### Opción 2A — Con Script Automático (Recomendado):
Abre PowerShell en el Ordenador 2 y ejecuta:
```powershell
powershell -ExecutionPolicy Bypass -File .\lab_pc2_estaciones.ps1
```
*(Te pedirá la IP de Central/Kafka y te dará a elegir entre `1 = Docker Compose` o `2 = Terminales Java interactivas`).*

#### Opción 2B — Con Comandos Manuales en Docker (Windows y Ubuntu):
* **En Windows (PowerShell):**
  ```powershell
  $env:IP_CENTRAL="<IP_CENTRAL>"; $env:PUERTO_CENTRAL="8080"
  $env:IP_KAFKA="<IP_CENTRAL>";   $env:PUERTO_KAFKA="9092"
  docker compose -f docker-compose.lab-ws.yml up -d --build
  ```
* **En Ubuntu / Linux (Bash):**
  ```bash
  IP_CENTRAL="<IP_CENTRAL>" PUERTO_CENTRAL="8080" IP_KAFKA="<IP_CENTRAL>" PUERTO_KAFKA="9092" docker compose -f docker-compose.lab-ws.yml up -d --build
  ```
* **Para entrar al teclado de `WS_01` y simular una fuga (`KO`) pulsando `ENTER`:**
  ```bash
  docker attach ws01_engine
  ```
  *(Para salir del `attach` sin apagar el contenedor: `Ctrl + P` seguido de `Ctrl + Q`).*

#### Opción 2C — Con Comandos Manuales en Java (sin Docker en el PC 2):
* **En Windows (separador `;`):**
  ```powershell
  javac -encoding UTF-8 -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" *.java

  # Terminal 1 (Monitor WS_01):
  java -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_WS_M 5001 <IP_CENTRAL> 8080 WS_01

  # Terminal 2 (Engine WS_01 - Pulsa ENTER aquí para activar/resolver Fuga KO/OK):
  java -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_WS_E <IP_CENTRAL> 9092 localhost 5001 WS_01
  ```
* **En Ubuntu / Linux (separador `:`):**
  ```bash
  javac -encoding UTF-8 -cp ".:lib/*:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar" *.java

  # Terminal 1 (Monitor WS_01):
  java -cp ".:lib/*:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar" WM_WS_M 5001 <IP_CENTRAL> 8080 WS_01

  # Terminal 2 (Engine WS_01 - Pulsa ENTER aquí para activar/resolver Fuga KO/OK):
  java -cp ".:lib/*:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar" WM_WS_E <IP_CENTRAL> 9092 localhost 5001 WS_01
  ```

---

### 👷 ORDENADOR 3: Operarios de Campo (`FO_01` y `FO_02`)

Sustituye `<IP_CENTRAL>` por la IP del Ordenador 1 (o la IP pública de AWS).

#### Opción 3A — Con Script Automático (Recomendado en Windows):
Abre PowerShell en el Ordenador 3 y ejecuta:
```powershell
powershell -ExecutionPolicy Bypass -File .\lab_pc3_operarios.ps1
```
*(Te pedirá la IP de Kafka y abrirá automáticamente las consolas interactivas de `FO_01` y `FO_02`).*

#### Opción 3B — Con Comandos Manuales en Docker (Windows y Ubuntu):
* **En Windows (PowerShell):**
  ```powershell
  $env:IP_KAFKA="<IP_CENTRAL>"; $env:PUERTO_KAFKA="9092"
  docker compose -f docker-compose.lab-fo.yml run --rm --build fo_01
  ```
* **En Ubuntu / Linux (Bash):**
  ```bash
  IP_KAFKA="<IP_CENTRAL>" PUERTO_KAFKA="9092" docker compose -f docker-compose.lab-fo.yml run --rm --build fo_01
  ```

#### Opción 3C — Con Comandos Manuales en Java (sin Docker en el PC 3):
* **En Windows (separador `;`):**
  ```powershell
  javac -encoding UTF-8 -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" *.java

  # Modo Interactivo (Muestra automáticamente las estaciones disponibles al arrancar o escribiendo LISTAR):
  java -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_FO <IP_CENTRAL> 9092 FO_01

  # Modo Fichero por Lotes directo:
  java -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_FO <IP_CENTRAL> 9092 FO_01 tareas.txt

  # Modo Orden Única directa (WS_01 durante 10 segundos):
  java -cp ".;lib/*;kafka-clients.jar;slf4j-api.jar;slf4j-simple.jar" WM_FO <IP_CENTRAL> 9092 FO_01 WS_01 10
  ```
* **En Ubuntu / Linux (separador `:`):**
  ```bash
  javac -encoding UTF-8 -cp ".:lib/*:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar" *.java

  # Modo Interactivo (Muestra automáticamente las estaciones disponibles al arrancar o escribiendo LISTAR):
  java -cp ".:lib/*:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar" WM_FO <IP_CENTRAL> 9092 FO_01
  ```

---

## 💻 ESCENARIO 2: Probar Todo en 1 Solo Ordenador (`localhost`)

Si quieres levantar toda la red en tu propio portátil sin depender de otros PCs ni de AWS:

1. **Todo en Docker con las 7 estaciones municipales (`WS_01` a `WS_07`)**:
   ```powershell
   docker compose up -d --build
   ```
   *(Abre `http://localhost:8081` y verás las 7 estaciones en verde `DISPONIBLE`).*

2. **O en 4 terminales Java locales con 1 clic (`arrancar_todo.ps1`)**:
   ```powershell
   docker compose up -d broker
   powershell -ExecutionPolicy Bypass -File .\arrancar_todo.ps1
   ```

3. **O levantar las 7 estaciones en procesos Java locales (`arrancar_7_estaciones.ps1`)**:
   ```powershell
   powershell -ExecutionPolicy Bypass -File .\arrancar_7_estaciones.ps1
   ```

---

## 🧪 Guion de Pruebas Paso a Paso para la Defensa en Laboratorio

Una vez tengas el Dashboard abierto (`http://<IP_CENTRAL>:8081`), las estaciones en el Ordenador 2 y el operario `FO_01` en el Ordenador 3:

### Prueba 1: Registro y Colores de Estado Iniciales
* Antes de encender el Ordenador 2, todas las estaciones aparecen en **GRIS (`DESCONECTADA`)** en el Dashboard Web.
* Al lanzar las estaciones en el Ordenador 2 (`WS_01` y `WS_02`), se registran por Sockets TCP (`REGISTRO#WS_01#...`) y pasan inmediatamente a **VERDE (`DISPONIBLE`)**.

### Prueba 2: Riego con Duración y Telemetría en Tiempo Real
* En la consola de `FO_01` (Ordenador 3), escribe:
  ```text
  WS_01#10
  ```
* `WM_Central` valida que `FO_01` existe en la tabla `operarios` de SQLite y que `WS_01` está `DISPONIBLE`.
* `WS_01` pasa a **VERDE PARPADEANDO (`REGANDO`)** en el Dashboard Web, mostrando en tiempo real el caudal (L/min), los litros acumulados y el operario `FO_01`.
* Al cumplirse los 10 segundos, `WM_Central` envía `PARAR_RIEGO#WS_01`, restaura `WS_01` a **VERDE (`DISPONIBLE`)** y `FO_01` recibe por pantalla:
  `FIN DE RIEGO en estación WS_01 | Resumen volumen total consumido: X.XX litros.`

### Prueba 3: Ejecución por Lotes (`tareas.txt`) con Pausa Reglamentaria de 4 Segundos
* En la consola de `FO_01`, escribe:
  ```text
  FILE#tareas.txt
  ```
* `WM_FO` envía el lote de órdenes de [`tareas.txt`](tareas.txt) a `WM_Central`, quien las encola y ejecuta secuencialmente dejando **exactamente 4 segundos de espera** entre el fin de un servicio y el comienzo del siguiente.

### Prueba 4: Simulación de Fuga (`KO`) en Pleno Riego y Recuperación (`OK`)
1. Pon a regar `WS_01` durante 30 segundos desde `FO_01`:
   ```text
   WS_01#30
   ```
2. En el Ordenador 2, ve a la terminal del Engine de `WS_01` (`docker attach ws01_engine` o ventana Java) y pulsa **`ENTER`**:
   * El Engine cambia su estado interno a `KO`.
   * En el siguiente segundo, el Monitor (`WM_WS_M`) detecta `KO` en el `STATUS_CHECK` y envía por socket `FUGA#WS_01` a `WM_Central`.
   * `WM_Central` **corta inmediatamente el agua** (`PARAR_RIEGO#WS_01`), cambia el estado a **ROJO (`FUGA`)** en la BD y en el Dashboard Web, y avisa en vivo al operario `FO_01`:
     `¡ALERTA! FUGA detectada en estación WS_01. Suministro cancelado inmediatamente.`
3. **Resolver la avería (`OK`)**:
   * Vuelve a pulsar **`ENTER`** en la terminal del Engine de `WS_01` (o pulsa el botón **"Resolver Avería (OK)"** en el Dashboard Web).
   * El Monitor detecta `OK`, envía `FIN_FUGA#WS_01` a Central y la estación vuelve a **VERDE (`DISPONIBLE`)**.

### Prueba 5: Gobierno Arbitrario desde `WM_Central` (Fuera de Servicio / Activación / Riego)
* Desde la consola de `WM_Central` (comandos `BLOQUEAR WS_02`, `ACTIVAR WS_02`, `REGAR WS_02 10`) o directamente desde los botones de cada tarjeta en el **Dashboard Web (`http://<IP_CENTRAL>:8081`)**:
  * Pulsa **"Bloquear (Fuera Servicio)"** en `WS_02`: la estación pasa a **NARANJA (`FUERA DE SERVICIO`)**.
  * Si `FO_01` intenta regar `WS_02#10`, recibe inmediatamente: `Riego DENEGADO en estación WS_02`.
  * Pulsa **"Activar (Disponible)"** en `WS_02`: la estación vuelve a **VERDE (`DISPONIBLE`)**.

---

## 🧹 Comandos para Apagar y Limpiar Contenedores al Terminar

```powershell
# Apagar estaciones (Ordenador 2):
docker compose -f docker-compose.lab-ws.yml down

# Apagar Central y Kafka local (Ordenador 1):
docker compose -f docker-compose.cloud.yml down

# Apagar escenario completo de 1 solo PC:
docker compose down
```