# Memoria Práctica 2 – Sistemas Distribuidos (Curso 2026/2027)
## Water Management Network (Release 1)

**Autores:**
* Carlos Oliva Castillo (`50384072L`)
* Nael Alba (`48804733K`)

**Repositorio GitHub:** https://github.com/CarlosOlivaCastillo/Water-Management-Network  
**Imágenes Docker Hub:** `carlosolivaa/practica-aws:latest` y `carlosolivaa/wm-central:latest`

---

## Índice

1. Introducción y arquitectura general
2. Explicación de los módulos implementados
3. Formato de los mensajes (Sockets y Kafka)
4. Puesta en marcha y despliegue (AWS Cloud y ordenadores del laboratorio)
5. Pruebas realizadas y capturas de funcionamiento

---

## 1. Introducción y arquitectura general

En esta práctica hemos implementado el sistema distribuido **Water Management Network**, cuyo objetivo es gestionar desde una central el riego de distintas estaciones repartidas por los parques de una ciudad, controlando tanto las órdenes de los operarios de campo como las posibles fugas o averías en tiempo real.

Para cumplir con lo que pide el enunciado, el sistema utiliza dos formas de comunicación distintas según el tipo de mensaje:

* **Sockets TCP (`ServerSocket` y `Socket`)**: Se usan para la comunicación directa entre el Engine y el Monitor de cada estación (comprobando cada segundo que no haya fugas) y entre el Monitor y la Central (para registrar la estación al arrancar y para avisar inmediatamente si se produce una avería o si ya se ha solucionado). Todos los mensajes por sockets van empaquetados con el formato `<STX><DATA><ETX><LRC>` para comprobar que no llegan corruptos.
* **Colas de mensajes con Apache Kafka**: Se usan para toda la parte de órdenes de riego y envío de telemetría. De esta forma, los operarios (`WM_FO`) mandan sus peticiones a Central por Kafka, Central manda la orden de abrir o cerrar el grifo al Engine (`WM_WS_E`), y el Engine va publicando cada segundo los litros consumidos mientras está regando.

El esquema general del despliegue que hemos montado es el siguiente:

```text
+-------------------------------------------------------------------------+
|                        ENTORNO CLOUD (AWS EC2)                          |
|                                                                         |
|   +-----------------------+          +------------------------------+   |
|   | Base de Datos SQLite  | <------> |         WM_Central           |   |
|   | (watermanagement.db)  |          | - Servidor Sockets (:8080)   |   |
|   | - Tabla estaciones    |          | - Panel Web (:8081)          |   |
|   | - Tabla operarios     |          +--------------+---------------+   |
|   +-----------------------+                         |                   |
|                                                     v                   |
|                                      +--------------+---------------+   |
|                                      |  Broker Apache Kafka (:9092) |   |
|                                      +--------------+---------------+   |
+-----------------------------------------------------|-------------------+
                                                      |
                  +-----------------------------------+-------------------+
                  |                                                       |
                  v (Kafka :9092)                                         v (Sockets :8080 y Kafka :9092)
+-----------------+--------------------+                +-----------------+--------------------+
|        ORDENADOR LAB 1 (FO)          |                |        ORDENADOR LAB 2 (WS)          |
|                                      |                |                                      |
|  - Operario WM_FO 1 (FO_01)          |                |  - Estación WS_01 (Monitor + Engine) |
|  - Operario WM_FO 2 (FO_02)          |                |  - Estación WS_02 (Monitor + Engine) |
+--------------------------------------+                +--------------------------------------+
```

---

## 2. Explicación de los módulos implementados

Hemos separado el código en 8 clases dentro del proyecto para que cada parte tenga su responsabilidad clara:

### 2.1. `ProtocoloUtil.java`
Es una clase auxiliar que utilizan todos los programas que hablan por sockets (`WM_WS_E`, `WM_WS_M` y `HiloServidor`). Tiene dos métodos principales:
* `empaquetar(String mensaje)`: Recorre los caracteres del texto haciendo un `XOR` para calcular el carácter de control `LRC` y devuelve el string con `STX` (`0x02`) delante, `ETX` (`0x03`) detrás y el `LRC` al final.
* `desempaquetar(String trama)`: Comprueba que el mensaje tenga el formato correcto, extrae el texto y vuelve a calcular el `LRC`. Si no coincide, lanza una excepción para responder `<NACK>`.

### 2.2. `WM_Central.java`
Es el servidor principal. Al arrancar recibe el puerto de escucha de sockets, la IP y puerto de Kafka y (opcionalmente) el puerto de la web:
`java WM_Central <puerto_central> <ip_kafka> <puerto_kafka> [puerto_web]`

Lo que hace al iniciarse es:
1. Inicializar la base de datos SQLite poniendo todas las estaciones en estado `DESCONECTADA` hasta que sus monitores se vayan conectando.
2. Arrancar el servidor web en el puerto `8081` y la consola de comandos para poder controlar las estaciones manualmente desde Central (`BLOQUEAR`, `ACTIVAR`, `REGAR`, `PARAR`).
3. Abrir el `ServerSocket` en el puerto `8080`. Por cada Monitor que se conecta, lanza un hilo `HiloServidor`.
4. Arrancar un productor y dos consumidores de Kafka:
   * Uno escucha el topic `peticiones_riego` (donde escriben los operarios `WM_FO`). Cuando llega un `START_RIEGO`, comprueba en la BD que el operario exista y que la estación esté `DISPONIBLE`. Si todo está bien, envía `INICIAR_RIEGO` al Engine, responde `ACK` al operario y, si el riego tiene duración limitada, programa una tarea con `ScheduledExecutorService` para cortar el agua automáticamente al terminar los segundos indicados.
   * Si un operario envía un lote de órdenes desde un fichero (separadas por `;`), Central las guarda en una cola por operario (`colasFO`) y conforme termina una orden espera 4 segundos (`espera(idFO)`) antes de ejecutar la siguiente.
   * El segundo consumidor escucha `telemetria-estaciones` para ir guardando y mostrando el caudal y el volumen acumulado en tiempo real.

### 2.3. `GestorCentral.java`
Gestiona toda la conexión con la base de datos SQLite (`db/watermanagement.db`). Hemos usado el patrón Singleton y métodos `synchronized` porque a la base de datos acceden a la vez varios hilos de sockets y los hilos de Kafka.
* Crea dos tablas si no existen: `estaciones` (con `idWS`, `ubicacion`, `estado`, `idFO`, `duracion` y `fecha_inicio`) y `operarios` (con los operarios dados de alta como `FO_01`, `FO_02`, `FO_03`, `CENTRAL` y `WEB_PANEL`).
* Tiene los métodos para registrar estaciones, autorizar o finalizar riegos, bloquear/activar estaciones y consultar el listado completo para enviárselo a los operarios o al panel web.

### 2.4. `HiloServidor.java`
Atiende la conexión TCP de un Monitor (`WM_WS_M`) concreto:
* Cuando recibe `REGISTRO#<idWS>#<ubicacion>`, da de alta la estación en la base de datos y la pasa a `DISPONIBLE`.
* Cuando recibe `FUGA#<idWS>`, pasa la estación a estado `FUGA`. Si en ese momento estaba `REGANDO`, llama a `WM_Central.cortarAguaFuga(...)` para mandar inmediatamente `PARAR_RIEGO` por Kafka al Engine, cancelar el temporizador y avisar al operario que tenía el riego activo.
* Cuando recibe `FIN_FUGA#<idWS>`, vuelve a poner la estación en `DISPONIBLE`.
* En el bloque `finally`, si el Monitor se cierra o se pierde la conexión de red, actualiza el estado de esa estación a `DESCONECTADA`.

### 2.5. `ServidorWebCentral.java`
Para poder ver el mapa de estaciones y los colores que pide el enunciado sin complicar la instalación, hemos usado el servidor HTTP incluido en Java (`com.sun.net.httpserver.HttpServer`) en el puerto `8081`.
Muestra las 7 estaciones con los colores indicados en el guion:
* **Verde (`DISPONIBLE`)**: Estación conectada y lista para regar.
* **Verde parpadeando (`REGANDO`)**: Muestra el operario que la ha activado, el caudal actual y los litros acumulados.
* **Rojo (`FUGA`)**: Avería detectada por el Monitor.
* **Naranja (`FUERA DE SERVICIO`)**: Estación bloqueada manualmente desde Central.
* **Gris (`DESCONECTADA`)**: El Monitor aún no se ha conectado o se ha caído.

Además, desde las tarjetas de la web también se pueden probar las acciones de Central (iniciar riego, bloquear a fuera de servicio, activar o resolver avería).

### 2.6. `WM_WS_M.java` (Monitor de la estación)
Se lanza indicando el puerto local para su Engine, la IP/puerto de Central y el ID de la estación:
`java WM_WS_M <Puerto_Engine> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]`
1. Primero se conecta por socket a `WM_Central` (hace hasta 5 intentos por si Central tarda un poco más en levantar) y se registra enviando `REGISTRO#<idWS>#<ubicacion>`.
2. Después abre su `ServerSocket` local dentro de un bucle `while (true)` esperando a que se conecte su `WM_WS_E`.
3. Una vez conectado el Engine, le envía cada segundo un `STATUS_CHECK`. Si el Engine responde `KO` o se cierra la conexión con el Engine, el Monitor envía `FUGA#<idWS>` a Central, pero el Monitor se queda abierto esperando a que el Engine vuelva a conectarse o responda `OK` (momento en el que envía `FIN_FUGA#<idWS>`).

### 2.7. `WM_WS_E.java` (Engine de la estación)
Simula la electroválvula y el caudalímetro de la estación:
`java WM_WS_E <IP_Kafka> <Puerto_Kafka> <IP_Monitor> <Puerto_Monitor> [ID_WS]`
1. Se conecta por socket a su Monitor local para responder `OK` o `KO` a cada `STATUS_CHECK`.
2. Escucha el topic `ordenes-riego` de Kafka. Cuando recibe `INICIAR_RIEGO#<idWS>`, pone `regando = true` y su hilo productor empieza a enviar cada segundo al topic `telemetria-estaciones` el caudal y los litros acumulados. Cuando recibe `PARAR_RIEGO#<idWS>`, cierra la válvula y deja de mandar telemetría.
3. Tiene un hilo de lectura por teclado para las pruebas en laboratorio: pulsando `ENTER` (o `1`) se simula una fuga (`KO`) y volviendo a pulsar `ENTER` se arregla la fuga (`OK`). También permite iniciar (`2`) o parar (`3`) el riego desde la propia estación.

### 2.8. `WM_FO.java` (Operario de campo)
Es la aplicación de consola para los operarios. Se conecta únicamente por Kafka:
`java WM_FO <IP_Kafka> <Puerto_Kafka> <ID_FO> [Ruta_Fichero | <ID_WS> <Duracion>]`
* Al arrancar en modo interactivo, envía automáticamente `LISTAR#<idFO>` a Central para mostrar en pantalla la tabla de estaciones y cuáles están `DISPONIBLE` (también se puede consultar en cualquier momento escribiendo `LISTAR` o `DISPONIBLES`).
* Permite pedir riegos con duración (`WS_01#10`), riegos indefinidos (`WS_01`), parar un riego en curso (`STOP#WS_01`) o cargar un fichero de tareas (`FILE#tareas.txt`).
* Su hilo consumidor escucha `respuestas_riego` filtrando por el `idFO` del operario para mostrar si el riego ha sido autorizado (`ACK`), denegado (`NACK`), cancelado por avería (`FUGA`) o ha terminado normalmente indicando los litros totales gastados (`FIN_RIEGO`).

---

## 3. Formato de los mensajes (Sockets y Kafka)

### 3.1. Mensajes por Sockets TCP (todos encapsulados con `<STX><DATA><ETX><LRC>`)

| Quién envía | Quién recibe | Contenido (`<DATA>`) | Respuesta | Qué hace |
| :--- | :--- | :--- | :--- | :--- |
| `WM_WS_M` | `WM_Central` | `REGISTRO#WS_01#Ubicacion` | `<ACK>` / `<NACK>` | Registra la estación al conectarse y la pone en `DISPONIBLE`. |
| `WM_WS_M` | `WM_Central` | `FUGA#WS_01` | `<ACK>` / `<NACK>` | Avisa de fuga en el Engine. Pasa a `FUGA` y corta el riego si estaba activo. |
| `WM_WS_M` | `WM_Central` | `FIN_FUGA#WS_01` | `<ACK>` / `<NACK>` | Avisa de que el Engine vuelve a dar `OK`. Pasa a `DISPONIBLE`. |
| `WM_WS_M` | `WM_WS_E` | `STATUS_CHECK` | `OK` / `KO` | Comprobación cada 1 segundo del estado del Engine. |

### 3.2. Topics y mensajes en Apache Kafka

| Topic | Productor | Consumidor | Formato del mensaje |
| :--- | :--- | :--- | :--- |
| `peticiones_riego` | `WM_FO`, `WM_WS_E` | `WM_Central` | `START_RIEGO#<idWS>#<idFO>#<dur>` \| `STOP_RIEGO#<idWS>#<idFO>` \| `LISTAR#<idFO>` |
| `respuestas_riego` | `WM_Central` | `WM_FO` | `ACK#<idWS>` \| `NACK#<idWS>` \| `FUGA#<idWS>` \| `FIN_RIEGO#<idWS>#<litros>` \| `LISTA#<datos>` |
| `ordenes-riego` | `WM_Central` | `WM_WS_E` | `INICIAR_RIEGO#<idWS>` \| `PARAR_RIEGO#<idWS>` \| `PARAR_RIEGO#ALL` |
| `telemetria-estaciones` | `WM_WS_E` | `WM_Central` | `TELEMETRIA#<idWS>#<caudal>#<volumenAcumulado>#<timestamp>` |

---

## 4. Puesta en marcha y despliegue (AWS Cloud y ordenadores del laboratorio)

### 4.1. Integración Continua (GitHub Actions -> Docker Hub)
Hemos configurado el fichero `.github/workflows/deploy.yml` para que cada vez que hacemos `push` a las ramas `main` o `develop`, GitHub Actions compile el `Dockerfile` de la Central y suba la imagen actualizada a nuestra cuenta de Docker Hub (`carlosolivaa/practica-aws:latest`).

### 4.2. Despliegue de Central y Kafka en AWS EC2
Siguiendo el tutorial de AWS Academy Learner Lab:
1. Creamos una instancia EC2 con **Ubuntu 24.04 (`t3.micro`)** en la región `us-east-1` usando el par de claves `vockey` (`labsuser.pem`).
2. En el Security Group abrimos los puertos **22** (SSH), **9092** (Kafka), **8080** (Sockets Central) y **8081** (Panel Web).
3. *Problema encontrado en AWS y solución:* Al probar Kafka en la instancia `t3.micro` (que solo tiene 1 GB de RAM), el contenedor de Kafka se cerraba al arrancar por falta de memoria (`Cannot allocate memory`). Lo solucionamos añadiendo un fichero de intercambio (`/swapfile`) de 2 GB en Ubuntu y limitando el heap de Kafka con `-e KAFKA_HEAP_OPTS="-Xmx350M -Xms350M"`.
4. Además, dejamos configurado un servicio en `/etc/systemd/system/wm-auto.service` que ejecuta `/usr/local/bin/start-wm.sh` al encender la instancia, de modo que detecta sola la IP pública del día (`curl -s http://checkip.amazonaws.com`) y levanta los contenedores `kafka` y `wm_central` automáticamente.

Comandos ejecutados en la instancia de AWS:
```bash
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
```

### 4.3. Ejecución en los ordenadores del laboratorio (PC Estaciones y PC Operarios)

Tanto si Central está en AWS como si se ejecuta en un tercer ordenador del laboratorio (`lab_pc1_central.ps1`), en los otros dos ordenadores basta con indicar la IP donde corre Central/Kafka:

* **En el Ordenador de Estaciones (`WS_01` y `WS_02`):**
  * Con Docker:
    ```powershell
    $env:IP_CENTRAL="<IP_CENTRAL>"; $env:PUERTO_CENTRAL="8080"
    $env:IP_KAFKA="<IP_CENTRAL>";   $env:PUERTO_KAFKA="9092"
    docker compose -f docker-compose.lab-ws.yml up -d --build
    ```
  * O directamente con el script interactivo: `.\lab_pc2_estaciones.ps1`

* **En el Ordenador de Operarios (`FO_01` y `FO_02`):**
  * Con Docker:
    ```powershell
    $env:IP_KAFKA="<IP_CENTRAL>"; $env:PUERTO_KAFKA="9092"
    docker compose -f docker-compose.lab-fo.yml run --rm --build fo_01
    ```
  * O directamente con el script interactivo: `.\lab_pc3_operarios.ps1`

---

## 5. Pruebas realizadas y capturas de funcionamiento

Durante las pruebas hemos comprobado todos los casos que pide el enunciado:

1. **Despliegue automático CI/CD y contenedores en AWS EC2:**
   * Comprobamos cómo al hacer `git push`, GitHub Actions construye y sube la imagen a Docker Hub, y cómo en la instancia EC2 de AWS corren los contenedores `kafka` y `wm_central` mostrando el panel web en `http://<IP_AWS>:8081` con todas las estaciones inicialmente en gris (`DESCONECTADA`).
   *(Insertar Captura 1 y Captura 2)*

2. **Conexión de las estaciones (`WS_01` y `WS_02`) y consulta desde `WM_FO`:**
   * Al lanzar las estaciones desde el PC del laboratorio apuntando a la IP de Central, se registran por sockets y pasan inmediatamente a color verde (`DISPONIBLE`). Al abrir `WM_FO`, el operario recibe la lista de estaciones y ve que `WS_01` y `WS_02` están disponibles para regar.
   *(Insertar Captura 3)*

3. **Riego temporizado, telemetría en tiempo real y ejecución por fichero (`tareas.txt`):**
   * Desde `FO_01` lanzamos `WS_01#10` y después `FILE#tareas.txt`. Durante el riego la estación parpadea en verde (`REGANDO`), el panel web muestra el caudal y los litros acumulados segundo a segundo, y al terminar llega a la consola del operario el mensaje `FIN DE RIEGO` con el volumen total consumido, esperando 4 segundos antes de pasar a la siguiente orden del fichero.
   *(Insertar Captura 4)*

4. **Simulación de fuga (`KO`), corte de agua y bloqueo desde Central (`FUERA DE SERVICIO`):**
   * Mientras `WS_01` está regando, pulsamos `ENTER` en la consola de `ws01_engine` (o cerramos el Engine). El Monitor detecta el fallo en el siguiente segundo, envía `FUGA#WS_01` a Central, la estación se pone en rojo (`FUGA`), se corta el suministro inmediatamente y `FO_01` recibe la alerta de cancelación por fuga. Al volver a pulsar `ENTER` en el Engine, el Monitor detecta `OK` y devuelve la estación a `DISPONIBLE`. Por último, probamos a bloquear `WS_02` desde Central comprobando que pasa a naranja (`FUERA DE SERVICIO`) y rechaza peticiones de riego con `NACK`.
   *(Insertar Captura 5)*
