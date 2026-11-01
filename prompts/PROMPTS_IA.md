# Registro de Prompts de IA – Water Management Network (Curso 2026/2027)

En este documento se recogen, separados por cada integrante del grupo, las consultas puntuales realizadas a la IA para depurar errores concretos durante el desarrollo y despliegue de la práctica.

---

## 1. Prompts de Carlos Oliva Castillo (`50384072L`)

### Prompt 1 – Fallo en el cálculo del LRC cuando el mensaje contiene varios delimitadores
> **Prompt:** *"Ya tengo hecha mi clase ProtocoloUtil.java con los métodos empaquetar y desempaquetar usando <STX> (0x02) y <ETX> (0x03) y el XOR carácter a carácter. Me funciona bien con tramas normales, pero en una prueba me ha dado fallo de índice al hacer indexOf(ETX) si el carácter resultante del XOR coincide casualmente con 0x03. ¿Basta con cambiar indexOf(ETX) por lastIndexOf(ETX) al extraer la posición final de los datos?"*
* **Para qué sirvió:** Cambié `indexOf(ETX)` por `lastIndexOf(ETX)` en mi método `desempaquetar()` de `ProtocoloUtil.java` para que nunca confunda el byte del LRC con el cierre de trama.

### Prompt 2 – El Monitor (WM_WS_M) se cerraba solo cuando mataba el Engine con Ctrl+C
> **Prompt:** *"En mi código de WM_WS_M.java tengo un ServerSocket que hace accept() al Engine y entra en un while(true) enviando STATUS_CHECK cada segundo. Funciona bien cuando pulso ENTER en el Engine porque me devuelve KO, pero si cierro la terminal del Engine con Ctrl+C me salta EOFException en readUTF(), entra en el catch, manda FUGA a Central y se termina el programa del Monitor. ¿Cómo coloco el try-catch dentro del bucle del ServerSocket para que al caer el Engine solo cierre ese Socket cliente y se quede esperando otro accept() sin apagar el Monitor?"*
* **Para qué sirvió:** Reorganicé mi bucle en `WM_WS_M.java` dejando `skServidor.accept()` en un `while(true)` exterior y capturando la desconexión del Engine dentro de `iniciarBucleDeSalud()`, de forma que el Monitor avisa de la fuga pero sigue vivo esperando a que el Engine vuelva a arrancar.

### Prompt 3 – Decimales con coma en vez de punto al formatear la telemetría en Windows
> **Prompt:** *"En el hilo de telemetría de mi WM_WS_E.java estoy armando el mensaje de Kafka con String.format('TELEMETRIA#%s#%.2f#%.2f#%d', idWS, caudal, volumen, timestamp). Al probarlo en mi Windows me saca '12,50' con coma porque tengo el sistema en español, y luego en Central al hacer Double.parseDouble() me salta NumberFormatException. ¿Cómo fuerzo en String.format que use siempre punto decimal sin cambiar el idioma del PC?"*
* **Para qué sirvió:** Añadí `Locale.US` como primer parámetro de `String.format(Locale.US, ...)` en `WM_WS_E.java` para evitar el `NumberFormatException` al parsear el caudal en Central.

### Prompt 4 – Error de CORS y lectura de parámetros POST en HttpServer de Java
> **Prompt:** *"He montado un pequeño servidor web en ServidorWebCentral.java con com.sun.net.httpserver.HttpServer para ver el mapa de las estaciones. Los GET de /api/estaciones me van bien, pero al pulsar el botón de regar que hace un fetch POST desde JavaScript no me lee los parámetros del body en HttpExchange. ¿Cómo se lee el InputStream del request body en HttpExchange sin librerías externas?"*
* **Para qué sirvió:** Implementé la lectura de `exchange.getRequestBody()` con `readAllBytes()` y mi función auxiliar `parseFormBody()` en `ServidorWebCentral.java`.

### Prompt 5 – Kafka se muere con error "Cannot allocate memory (errno=12)" en la instancia t3.micro de AWS
> **Prompt:** *"He subido mi contenedor de WM_Central y el de Kafka a la instancia EC2 de AWS Academy (es una t3.micro con 1 GB de RAM). Al hacer docker run del contenedor de Kafka se apaga a los 3 segundos y en docker logs me sale: OpenJDK 64-Bit Server VM warning: os::commit_memory failed; error='Not enough space' (errno=12). ¿Qué variable de entorno tengo que pasarle a la imagen de Kafka para bajarle la RAM que reserva por defecto?"*
* **Para qué sirvió:** Le pasé `-e KAFKA_HEAP_OPTS="-Xmx350M -Xms350M"` en el `docker run` de Kafka y creé un fichero `/swapfile` de 2 GB en el Ubuntu de AWS para que no se quedara sin memoria.

### Prompt 6 – Los clientes del laboratorio intentaban conectarse a la IP antigua de AWS tras reiniciar la instancia
> **Prompt:** *"He apagado y vuelto a encender la instancia EC2 de AWS y me ha cambiado la IP pública a 54.205.199.193. Cuando arranco mi WM_FO en mi ordenador pasándole la IP nueva 54.205.199.193:9092, en los logs veo que conecta al principio pero enseguida pone 'Discovered group coordinator 18.212.204.232:9092' (que era la IP de ayer) y da timeout. ¿Por qué Kafka me devuelve la IP vieja si le estoy pasando la nueva por argumentos?"*
* **Para qué sirvió:** Entendí que Kafka devuelve a los clientes la IP configurada en `KAFKA_CFG_ADVERTISED_LISTENERS` al crear el contenedor, así que hice el script `arrancar_aws.sh` con `curl -s http://checkip.amazonaws.com` para recrear el contenedor con la IP pública del día al encender AWS.

### Prompt 7 – Error UnsupportedClassVersionError (version 70.0 vs 52.0) al pasar el .jar al otro ordenador
> **Prompt:** *"He compilado mis clases en mi portátil y al pasarle el proyecto a mi compañero para probarlo en dos ordenadores nos salta: UnsupportedClassVersionError: WM_FO has been compiled by a more recent version of the Java Runtime (class file version 70.0), this version only recognizes up to 52.0. ¿Qué flag le pongo a mi javac para que genere los .class compatibles con Java 8 y no tengamos que recompilar en los ordenadores del laboratorio?"*
* **Para qué sirvió:** Compilé el proyecto con `javac --release 8` para generar el bytecode en versión `52.0` dentro de `lib/WaterManagement.jar`, compatible con cualquier versión de Java instalada en el laboratorio.

---

## 2. Prompts de Nael Alba (`48804733K`)

### Prompt 1 – Doble ejecución de consultas SQLite al inicializar el patrón Singleton en `GestorCentral`
> **Prompt:** *"En mi clase `GestorCentral.java` he implementado un patrón Singleton con `getInstance()`. Dentro del constructor privado llamo a `crearTablasIniciales()` y a `cargarDatos()` (que hace un UPDATE masivo poniendo las estaciones en 'DESCONECTADA'). Sin embargo, al arrancar `WM_Central.java` hago `if(GestorCentral.getInstance().cargarDatos())` para verificar que la base de datos responde antes de levantar los sockets. ¿Esto provoca que la consulta UPDATE se ejecute dos veces seguidas en disco, y por qué es mejor separar la inicialización de la conexión en el constructor de la carga de estados?"*
* **Para qué sirvió:** Comprendí el flujo exacto de la instanciación perezosa (*lazy initialization*) del Singleton y eliminé la llamada redundante a `cargarDatos()` dentro del constructor de `GestorCentral.java`, manteniendo únicamente la verificación booleana en el `main` de `WM_Central.java`.

### Prompt 2 – Desacoplamiento entre la capa de persistencia (`GestorCentral`) y las acciones de red (`WM_Central`)
> **Prompt:** *"En el método `procesarPeticion()` de mi `GestorCentral.java`, cuando recibo un mensaje de `FUGA#idWS` o `BLOQUEAR#idWS`, necesito saber si la estación estaba en estado `REGANDO` para mandar una orden de parada por Kafka (`PARAR_RIEGO`) y avisar al operario. Si llamo directamente a `WM_Central.cortarAguaFuga(idWS)` desde dentro de `GestorCentral`, acoplo la base de datos con la lógica de Kafka. ¿Cómo puedo estructurar la respuesta de `procesarPeticion()` para que `GestorCentral` solo actualice SQLite y sea `HiloServidor` o `WM_Central` quien decida si tiene que enviar mensajes por red?"*
* **Para qué sirvió:** Diseñé un protocolo de retorno compuesto (`<ACK>#R` si estaba regando y `<ACK>#D` si estaba disponible) en los casos `FUGA` y `BLOQUEAR` de `GestorCentral.java`, logrando desacoplar la base de datos SQLite de los productores de Kafka.


### Prompt 3 – Error en la condición booleana al filtrar mensajes de Kafka por clave (`record.key()`) en `WM_FO`
> **Prompt:** *"En el hilo consumidor de mi `WM_FO.java` me suscribo al topic `respuestas_riego` con `group.id` igual a `'grupo-fo-' + idFO`. Como la Central publica las respuestas usando el `idFO` como clave del `ProducerRecord`, he puesto este filtro en el bucle: `if(record.key() != null || !record.key().equals(idFO)) continue;`. Al probarlo, el operario ignora absolutamente todos los mensajes aunque vayan dirigidos a su ID. ¿Qué falla en la lógica booleana de ese `if` y cómo actúa el cortocircuito con los nulos?"*
* **Para qué sirvió:** Detecté que al usar `!= null` con un operador `||` (OR), cualquier mensaje válido con clave evaluaba la primera condición como `true` y ejecutaba el `continue`. Lo corregí a `if (record.key() == null || !record.key().equals(idFO))` para descartar nulos de forma segura sin producir `NullPointerException`.

### Prompt 4 – Sincronización entre el hilo lector de ficheros y el consumidor de Kafka usando `CompletableFuture`
> **Prompt:** *"Cuando mi `WM_FO.java` lee órdenes secuenciales desde un fichero en `ejecutarFichero()`, mi método `enviarKafka()` enviaba el mensaje y hacía un `Thread.sleep(4000)` fijo, lo que provocaba que se lanzara la siguiente línea del fichero antes de que terminara el riego actual en la Central. Me han sugerido usar un `CountDownLatch` o un `CompletableFuture<String> esperaRespuesta` compartido con el hilo consumidor. ¿Cómo funciona teóricamente el bloqueo con `esperaRespuesta.get()` y el desbloqueo con `esperaRespuesta.complete()`, y cómo calculo el timeout para que no salte un error si el riego dura varios minutos pero tampoco se quede en deadlock si la Central se cae?"*
* **Para qué sirvió:** Sustituí la espera ciega por una barrera de sincronización real con `CompletableFuture`, despertando al hilo principal únicamente cuando el consumidor recibe `FIN_RIEGO`, `NACK` o `FUGA`, y ajustando un *timeout* dinámico en función de la duración del riego (`duracion + 1` minutos) antes de ejecutar los 4 segundos de pausa.

### Prompt 5 – Inconsistencia en la validación de riego indefinido (`duracion = 0`) y cierre prematuro por `setDaemon(true)`
> **Prompt:** *"Tengo dos dudas de depuración: 1) En `WM_FO.java`, cuando el usuario escribe solo `WS_01` para riego indefinido, mi código llama internamente a `ejecutarTarea(idWS, '0')`, pero si el usuario escribe explícitamente `WS_01#0`, pasa por mi método `numeroValido()` que tiene `return duracion > 0` y lo rechaza. 2) En `WM_Central.java`, si marco el `hiloSocket` y los hilos consumidores de Kafka con `setDaemon(true)` mientras aún no he puesto el bucle de la interfaz en el `main`, el programa se cierra nada más arrancar. ¿Por qué ocurre esto a nivel de la JVM?"*
* **Para qué sirvió:** Ajusté `numeroValido()` a `duracion >= 0` para dar coherencia a las órdenes de riego infinito explícitas e implícitas, y comprendí que la JVM se apaga automáticamente cuando solo quedan *Daemon Threads* vivos si el hilo principal (`main`) finaliza sin quedarse bloqueado en un menú o interfaz gráfica.

### Prompt 6 – Diseño teórico del patrón Request-Reply sobre Kafka para el flujo de comunicación y comando `LISTAR`
> **Prompt:** *"Necesito plantear el diagrama de flujo de comunicación y la implementación teórica para que un operario (`WM_FO`) pueda consultar las estaciones disponibles en la Central mediante un comando `LISTAR`. Siendo Kafka un bus asíncrono de mensajería y no una base de datos directa, ¿cómo se diseña un patrón Request-Reply sobre los topics `peticiones_riego` y `respuestas_riego` reutilizando `obtenerEstaciones()` de `GestorCentral`, y cómo debo serializar la lista en un solo String sin que colisione con el separador `#` que ya uso en el resto de tramas?"*
* **Para qué sirvió:** Definí la arquitectura de comunicación para peticiones de consulta en Kafka, utilizando un delimitador secundario (`@`) para empaquetar las tuplas de estaciones desde `WM_Central` y enrutando la respuesta de vuelta exclusivamente al operario solicitante mediante la clave `idFO`.

### Prompt 7 – Diseño del diagrama de comunicaciones entre Central, Monitor y Engine
> **Prompt:** *"Necesito generar un diagrama de secuencia y arquitectura en Mermaid.js que detalle exclusivamente el subsistema de la estación de riego. En mi proyecto (Water Management Network), la estación tiene dos componentes: el Engine (WM_WS_E) y el Monitor (WM_WS_M). Quiero que el diagrama muestre 3 flujos clave: 1) El Monitor actúa como cliente Socket hacia WM_Central enviando el `REGISTRO` y simultáneamente como servidor Socket aceptando la conexión del Engine para enviarle `STATUS_CHECK` por un hilo dedicado. 2) El flujo de riego normal, donde WM_Central envía `INICIAR_RIEGO` / `PARAR_RIEGO` por Kafka (topic `ordenes-riego`) y el Engine responde publicando en `telemetria-estaciones`. 3) El flujo de fallo: si el Engine cae abruptamente, el Monitor lo detecta por `EOFException` en el Socket, envía la trama de `FUGA` a HiloServidor en la Central, y WM_Central procede a cortar el agua enviando el paro por Kafka."*
* **Para qué sirvió:** Me permitió entender el flujo de comunicaciones entre los diferentes componentes del proyecto.