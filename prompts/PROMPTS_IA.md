# Registro de Prompts de IA – Water Management Network (Curso 2026/2027)

En este documento se recogen, separados por cada integrante del grupo, los prompts utilizados como apoyo técnico durante el desarrollo de la práctica.

---

## 1. Prompts de Carlos Oliva Castillo (`50384072L`)

*Tareas principales: Protocolo `<STX><DATA><ETX><LRC>` (`ProtocoloUtil`), módulos de estación (`WM_WS_M` y `WM_WS_E`), panel web en tiempo real (`ServidorWebCentral`), resiliencia ante caídas, Docker, CI/CD en GitHub Actions y despliegue Cloud en AWS EC2.*

### Prompt 1 – Diseño de ProtocoloUtil.java (Empaquetado <STX><DATA><ETX><LRC> y validación XOR)
> **Prompt:** *"Estoy implementando en Java la clase ProtocoloUtil para encapsular las tramas de sockets TCP entre WM_WS_E, WM_WS_M y WM_Central siguiendo el formato <STX><DATA><ETX><LRC> (con STX=0x02 y ETX=0x03). Ya tengo el bucle XOR carácter a carácter para calcular el LRC, pero quiero asegurarme de que al enviar por DataOutputStream.writeUTF() y recibir por readUTF() no haya problemas de indexado si el propio LRC coincide con algún carácter de control. ¿Cómo estructuro desempaquetar(String trama) buscando la última aparición de ETX (lastIndexOf) y lanzando IllegalArgumentException si el LRC calculado no coincide?"*
* **Aplicación en el proyecto:** Se utilizó para afinar el parseo seguro con indexOf(STX) y lastIndexOf(ETX) en ProtocoloUtil.java, garantizando que cualquier corrupción de bits devuelva <NACK>.

### Prompt 2 – Bucle de Heartbeat cada 1s entre Monitor (WM_WS_M) y Engine (WM_WS_E)
> **Prompt:** *"En mi implementación de WM_WS_M.java abro un ServerSocket local para que se conecte WM_WS_E y le envío STATUS_CHECK cada 1000ms. El problema que tengo al probar en dos terminales es que cuando cierro WM_WS_E con Ctrl+C, el Monitor captura la IOException, envía FUGA#<idWS> a Central, pero después sale del método y se cierra también el Monitor. ¿Cómo reorganizo el bucle accept() dentro de un while(true) manteniendo vivo el socket hacia WM_Central para que la estación permanezca en estado FUGA (rojo) y acepte la reconexión del Engine cuando vuelva a arrancarse?"*
* **Aplicación en el proyecto:** Se aplicó en WM_WS_M.java separando el ciclo de vida del socket de Central del bucle while(true) del ServerSocket local del Engine.

### Prompt 3 – Hilo no bloqueante de simulación y resolución de averías en caliente (WM_WS_E)
> **Prompt:** *"En WM_WS_E.java necesito que, mientras el hilo principal atiende los STATUS_CHECK por socket y otro hilo consume de Kafka, el usuario pueda pulsar ENTER en consola para alternar el estado de avería (KO <-> OK) o solicitar inicio/parada manual de riego sin bloquear el bucle de red. ¿Cuál es la forma más limpia de sincronizar la bandera boolean averiaSimulada entre el hilo Daemon de Scanner(System.in) y los hilos de socket/Kafka en Java 17?"*
* **Aplicación en el proyecto:** Se implementó usando variables volatile (averiaSimulada, monitorConectado, regando) y un hilo lector de teclado configurado como daemon en WM_WS_E.java.

### Prompt 4 – Productor y consumidor asíncrono de Kafka en WM_WS_E (ordenes-riego y telemetria-estaciones)
> **Prompt:** *"Tengo configurada la librería kafka-clients en WM_WS_E.java. Necesito dos hilos concurrentes: uno con KafkaConsumer suscrito a 'ordenes-riego' (con group.id único por estación 'grupo-estaciones-' + idWS) que procese INICIAR_RIEGO#<idWS>#<duracion> y PARAR_RIEGO#<idWS>, y otro hilo que, solo mientras regando==true, incremente el volumen acumulado y publique cada 1 segundo en 'telemetria-estaciones' la trama TELEMETRIA#<idWS>#<caudal>#<volumen>#<timestamp> usando Locale.US para los decimales."*
* **Aplicación en el proyecto:** Se integró en los hilos hiloKafka e hiloTelemetria de WM_WS_E.java, añadiendo un ShutdownHook para cerrar el KafkaProducer limpiamente.

### Prompt 5 – Persistencia local de volumen y autonomía del Engine ante caída de WM_Central
> **Prompt:** *"Según la rúbrica de resiliencia de la práctica, si WM_WS_E cae durante un riego no puede perder el agua que ya haya suministrado, y si quien cae es WM_Central durante un riego temporizado, la estación debe seguir regando hasta agotar su tiempo y parar por sí sola. ¿Cómo puedo persistir de forma ligera el volumenAcumulado en un fichero local (db/volumen_<idWS>.dat) cada segundo y llevar un contador local segundosRestantes en el hilo de telemetría como respaldo si no llega PARAR_RIEGO desde Central?"*
* **Aplicación en el proyecto:** Se programaron los métodos guardarVolumenPrevio/cargarVolumenPrevio y el decremento autónomo de segundosRestantes en WM_WS_E.java.

### Prompt 6 – Bloqueo de riegos manuales y remotos cuando cae el Monitor de la estación
> **Prompt:** *"En las pruebas de resiliencia, si mato el proceso WM_WS_M (Monitor) con Ctrl+C pero dejo vivo WM_WS_E (Engine), el Engine debe detener cualquier riego activo, mostrar que la estación está 'Averiada' y rechazar tanto órdenes de Kafka como intentos de iniciar riego manualmente desde su propio menú hasta que el Monitor vuelva a estar operativo. Revisa mi bucle de conexión Socket en WM_WS_E.java para añadir reconexión automática al Monitor y validar la bandera monitorConectado."*
* **Aplicación en el proyecto:** Se añadió la validación (averiaSimulada || !monitorConectado) tanto en el menú manual como en el consumidor de Kafka de WM_WS_E.java.

### Prompt 7 – Cuadro de Mandos Web embebido (ServidorWebCentral.java) sin dependencias externas
> **Prompt:** *"Quiero añadir a WM_Central una interfaz visual en tiempo real sin usar Spring ni librerías externas que compliquen el classpath, utilizando únicamente com.sun.net.httpserver.HttpServer del JDK en el puerto 8081. ¿Cómo estructuro los HttpHandler para exponer los endpoints REST (/api/estaciones, /api/riego, /api/estado, /api/emergencia) leyendo el estado desde GestorCentral y sirviendo un dashboard HTML/JS que actualice cada segundo los colores oficiales del enunciado (Verde Available, Verde parpadeando Watering con L/min, L acumulados e ID de operario, Rojo Leak, Naranja Out of Service y Gris Disconnected)?"*
* **Aplicación en el proyecto:** Se desarrolló ServidorWebCentral.java con el mapa topológico SVG de las 7 estaciones, gráfica de caudal en vivo y controles de gobierno.

### Prompt 8 – Gobierno arbitrario sobre una estación o todas (ALL) desde Central
> **Prompt:** *"En WM_Central.java ya tengo implementados los comandos para iniciar riego, bloquear (pasar a FUERA_DE_SERVICIO / Out of Service cortando cualquier riego activo) y activar (volver a DISPONIBLE) sobre una estación concreta. Para cumplir el apartado de la rúbrica que pide enviar estas órdenes de forma arbitraria a una o a TODAS las estaciones, ¿cómo extiendo procesarPeticionFO, bloquearEstacion y activarEstacion para que al recibir 'ALL' iteren sobre obtenerIdsEstaciones() de SQLite?"*
* **Aplicación en el proyecto:** Se implementó el soporte de 'ALL' tanto en la consola de comandos de WM_Central.java como en los botones globales del panel web.

### Prompt 9 – Reconexión automática de WM_WS_M y detección de caída de WM_Central
> **Prompt:** *"Necesito que si WM_Central se cierra abruptamente durante la ejecución y se vuelve a arrancar después, los monitores WM_WS_M que ya estaban encendidos lo detecten mostrando el error controlado 'Imposible conectar con WM_Central' y se reconecten y re-autentiquen automáticamente sin tener que reiniciar las estaciones. ¿Cómo implemento un hilo de heartbeat PING#<idWS> cada 2.5s con bloque synchronized sobre el socket de Central para no interferir con los envíos de FUGA?"*
* **Aplicación en el proyecto:** Se implementó en WM_WS_M.java mediante el objeto de bloqueo lockCentral y el hilo demonio Hilo-ReconexionCentral.

### Prompt 10 – Contenerización multi-etapa (Dockerfiles) y orquestación separada Cloud / Laboratorio
> **Prompt:** *"Tengo que preparar el despliegue en contenedores Docker separando el escenario físico de la práctica: por un lado la parte Cloud (WM_Central + SQLite + Kafka) y por otro los ordenadores del laboratorio (PC de estaciones WM_WS y PC de operarios WM_FO). ¿Cómo escribo los Dockerfiles multi-etapa con eclipse-temurin:17-jdk-alpine (compilación) y 17-jre-alpine (ejecución) parametrizando las IPs y puertos mediante variables de entorno en docker-compose.cloud.yml, docker-compose.lab-ws.yml y docker-compose.lab-fo.yml?"*
* **Aplicación en el proyecto:** Se crearon Dockerfile, Dockerfile.monitor, Dockerfile.engine, Dockerfile.fo y los tres archivos docker-compose por rol.

### Prompt 11 – Pipeline CI/CD en GitHub Actions (.github/workflows/deploy.yml) hacia Docker Hub
> **Prompt:** *"Siguiendo el tutorial de CI/CD de la asignatura, quiero configurar un workflow de GitHub Actions en .github/workflows/deploy.yml que se dispare en cada push a las ramas main y develop, autentique en Docker Hub mediante secrets.DOCKER_USERNAME y secrets.DOCKER_PASSWORD, construya la imagen de WM_Central y publique el tag latest."*
* **Aplicación en el proyecto:** Se configuró y verificó el workflow deploy.yml publicando automáticamente en carlosolivaa/practica-aws:latest y carlosolivaa/wm-central:latest.

### Prompt 12 – Diagnóstico de Out Of Memory (errno=12) de Kafka en instancia AWS EC2 t3.micro
> **Prompt:** *"Al desplegar el contenedor bitnamilegacy/kafka:3.7.0 en una instancia EC2 Ubuntu t3.micro de AWS Academy Learner Lab (que solo dispone de 1 GB de RAM), el contenedor se detiene a los pocos segundos y en docker logs aparece: os::commit_memory failed; error='Not enough space' (errno=12). Además, los clientes externos de los PCs del laboratorio no logran producir si KAFKA_CFG_ADVERTISED_LISTENERS tiene localhost. ¿Cómo limito el heap de la JVM de Kafka y configuro un swapfile de 2 GB junto con la IP pública dinámica en AWS?"*
* **Aplicación en el proyecto:** Se resolvió activando un /swapfile de 2 GB en la instancia EC2, fijando KAFKA_HEAP_OPTS='-Xmx350M -Xms350M' y anunciando la IP pública en KAFKA_CFG_ADVERTISED_LISTENERS.

### Prompt 13 – Servicio systemd en Ubuntu EC2 para auto-arranque con IP pública dinámica en AWS Learner Lab
> **Prompt:** *"Cada vez que reinicio la sesión de AWS Academy Learner Lab y enciendo la instancia EC2 Cloud-WM, AWS le asigna una IPv4 pública nueva, lo que invalida el KAFKA_CFG_ADVERTISED_LISTENERS anterior. Además, en Ubuntu 24.04 de AWS el endpoint 169.254.169.254 usa IMDSv2 y devuelve vacío con un curl simple. ¿Cómo creo un script /usr/local/bin/start-wm.sh registrado como servicio systemd (wm-auto.service) que obtenga la IP pública con checkip.amazonaws.com al arrancar la máquina y levante automáticamente los contenedores kafka y wm_central con la nueva IP?"*
* **Aplicación en el proyecto:** Se dejó instalado y habilitado wm-auto.service en la instancia EC2 de AWS para que el entorno Cloud arranque listo sin intervención manual.

---

## 2. Prompts de Nael Alba (`48804733K`)

*Tareas principales: Gestión central y persistencia SQLite (`GestorCentral`, `HiloServidor`, `WM_Central`), aplicación de operarios de campo (`WM_FO`) y colas de lotes.*

> **[ESPACIO RESERVADO PARA LOS PROMPTS DE NAEL ALBA (`48804733K`)]**
> * **Prompt 1:** *"..."*
> * **Prompt 2:** *"..."*
> * **Prompt 3:** *"..."*
