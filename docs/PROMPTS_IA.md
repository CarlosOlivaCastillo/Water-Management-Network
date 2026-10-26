# Registro de Prompts de Inteligencia Artificial - Water Management Network

> **Nota académica:** De acuerdo con las directrices del enunciado de la práctica (*Sobre el uso de herramientas de Inteligencia Artificial (IA)*, página 14), en este documento se recogen los *prompts* empleados como herramienta de apoyo durante el desarrollo, depuración, contenerización y despliegue del sistema distribuido **Water Management Network**.

---

## 1. Diseño del Protocolo de Sockets TCP y Verificación LRC
* **Prompt 1:**
  > *"Necesitamos implementar en Java una clase de utilidad `ProtocoloUtil` que empaquete y desempaquete mensajes de texto siguiendo el estándar `<STX><DATA><ETX><LRC>`, donde `STX = 0x02`, `ETX = 0x03` y `LRC` es el XOR byte a byte del contenido del mensaje. Si el LRC no coincide al desempaquetar, debe lanzar una excepción indicando fallo de integridad."*
* **Prompt 2:**
  > *"En el módulo `WM_WS_M` (Monitor) y `WM_WS_E` (Engine), necesitamos un bucle de comprobación de estado de salud cada 1 segundo por Sockets TCP enviando `STATUS_CHECK` y respondiendo `OK` o `KO`. Además, el Engine debe contar con un hilo lector de teclado para simular una avería (fuga) en caliente y poder resolverla posteriormente volviendo al estado `OK`."*

---

## 2. Concurrencia en Central y Base de Datos SQLite
* **Prompt 3:**
  > *"¿Cómo estructurar el patrón Singleton sincronizado en `GestorCentral.java` con `sqlite-jdbc` para almacenar las tablas `estaciones` (idWS, ubicacion, estado, idFO, duracion, fecha_inicio) y `operarios` (idFO, nombre), garantizando que al reiniciar `WM_Central` todas las estaciones pasen a estado `DESCONECTADA` hasta que sus respectivos monitores vuelvan a autenticarse?"*
* **Prompt 4:**
  > *"En `HiloServidor.java`, cuando se cierra abruptamente el socket de un Monitor `WM_WS_M`, necesitamos que el bloque `finally` actualice el estado de esa estación en SQLite a `DESCONECTADA` sin afectar a conexiones puntuales de otros clientes."*

---

## 3. Integración con Apache Kafka (Streaming de Eventos y Colas)
* **Prompt 5:**
  > *"Implementar en `WM_WS_E` un `KafkaConsumer` suscrito al topic `ordenes-riego` que filtre selectivamente los comandos `INICIAR_RIEGO#<idWS>` y `PARAR_RIEGO#<idWS>` (o `ALL`), y un hilo `KafkaProducer` que, mientras la electroválvula esté abierta, publique cada segundo en `telemetria-estaciones` la trama `TELEMETRIA#<idWS>#<caudal>#<volumenAcumulado>#<timestamp>`."*
* **Prompt 6:**
  > *"Configurar la comunicación asíncrona entre `WM_FO` (Field Operator) y `WM_Central` mediante los topics `peticiones_riego` y `respuestas_riego`, utilizando `CompletableFuture` en `WM_FO` para esperar la finalización del riego (`FIN_RIEGO#<idWS>#<litros>`), un rechazo (`NACK`) o un corte por avería (`FUGA`), respetando una espera de 4 segundos entre servicios al leer un fichero por lotes."*

---

## 4. Cuadro de Mandos Web (Dashboard HTTP en Tiempo Real)
* **Prompt 7:**
  > *"Crear una clase `ServidorWebCentral` utilizando `com.sun.net.httpserver.HttpServer` embebido en el JDK de Java para ofrecer un panel de monitorización en tiempo real en el puerto 8081 que muestre las 7 estaciones de riego con la paleta de colores oficial del enunciado (Verde = Disponible, Verde parpadeando = Regando con caudal L/min, volumen acumulado e ID de operario, Rojo = Fuga, Naranja = Fuera de Servicio, Gris = Desconectada) y permita iniciar riego, bloquear y activar estaciones."*

---

## 5. Contenerización con Docker y CI/CD en Cloud (AWS / Railway)
* **Prompt 8:**
  > *"Escribir Dockerfiles multi-stage basados en `eclipse-temurin:17-jdk-alpine` y `17-jre-alpine` para `WM_Central`, `WM_WS_M`, `WM_WS_E` y `WM_FO`, junto con los archivos `docker-compose.cloud.yml`, `docker-compose.lab-ws.yml` y `docker-compose.lab-fo.yml` para desplegar el escenario físico distribuido entre Cloud y dos PCs de laboratorio."*
* **Prompt 9:**
  > *"Configurar el workflow `.github/workflows/deploy.yml` de GitHub Actions para construir y publicar automáticamente la imagen Docker de `WM_Central` en Docker Hub en cada push a `main`, siguiendo la guía de CI/CD de AWS Labs de la asignatura."*
