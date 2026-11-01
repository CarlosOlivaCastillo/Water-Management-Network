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

> **[ESPACIO RESERVADO PARA LOS PROMPTS DE NAEL ALBA (`48804733K`)]**
> * **Prompt 1:** *"..."*
> * **Prompt 2:** *"..."*
> * **Prompt 3:** *"..."*
