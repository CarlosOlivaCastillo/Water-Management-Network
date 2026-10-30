# Prompts de IA utilizados en la Práctica 2

En este fichero recopilamos las consultas que hemos ido haciendo a la IA como apoyo durante el desarrollo de la práctica, principalmente para dudas de configuración de Kafka, SQLite, el servidor web embebido y el despliegue en Docker/AWS.

---

### 1. Empaquetado `<STX><DATA><ETX><LRC>` en Java
* *"Cómo puedo hacer una función en Java que le añada los bytes STX (0x02) y ETX (0x03) a un String y calcule el LRC al final haciendo XOR de todos los caracteres, y otra función para comprobarlo al recibirlo por un DataInputStream."*

### 2. Conexión con SQLite y concurrencia en Central
* *"Estamos haciendo el servidor central con varios hilos (uno por cada socket de estación y otro para Kafka) y usamos sqlite-jdbc. Cómo hacemos una clase Singleton en Java con métodos synchronized para que no dé error de database locked al actualizar el estado de las estaciones."*
* *"Cómo hacer en SQLite un UPDATE que cambie el estado de la estación y además ponga a NULL el idFO y la duración cuando pasa a DISPONIBLE, FUGA o DESCONECTADA."*

### 3. Consumidor y productor de Apache Kafka en Java
* *"Ejemplo sencillo en Java con kafka-clients para crear un KafkaProducer y un KafkaConsumer en un hilo aparte sin que bloquee el main, filtrando los mensajes por la key del registro."*
* *"Cómo usar ScheduledExecutorService en Java para programar que a los X segundos se corte el riego de una estación, pero guardando la referencia por si hay que cancelarlo antes cuando salta una fuga o un STOP manual."*

### 4. Panel web desde Java sin frameworks externos
* *"Cómo levantar un servidor web simple en el puerto 8081 usando solo com.sun.net.httpserver.HttpServer de Java para devolver un JSON con las estaciones de la base de datos y una página HTML que se refresque cada segundo con fetch."*

### 5. Docker, GitHub Actions y problemas en AWS Learner Lab
* *"Cómo hacer un Dockerfile con eclipse-temurin:17 para compilar varios archivos .java que usan librerías externas en una carpeta lib/ y ejecutarlo pasándole la IP de Kafka por variables de entorno."*
* *"Al lanzar el contenedor de bitnamilegacy/kafka en una instancia t3.micro de AWS EC2 se apaga solo y en docker logs sale Cannot allocate memory (errno=12). Cómo reducimos la memoria que pide Kafka al arrancar y cómo añadimos swap en Ubuntu."*
* *"Cómo configurar el archivo .github/workflows/deploy.yml para que al hacer push al repositorio construya la imagen Docker y la suba a Docker Hub usando secrets."*
