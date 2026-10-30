import java.io.*;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

public class WM_Central {

    private static int puertoEscucha;
    private static String ipKafka;
    private static int puertoKafka;
    private static ServerSocket skServidor;
    private static volatile boolean ejecutando = true;

    // Productor y consumidores de Kafka
    private static KafkaProducer<String, String> productorKafka;
    private static KafkaConsumer<String, String> consumidorTelemetria;
    private static KafkaConsumer<String, String> consumidorFO;

    // Planificador de tareas para controlar los tiempos de riego
    private static final ScheduledExecutorService temporizadorRiego = Executors.newScheduledThreadPool(4);
    private static final ConcurrentHashMap<String, ScheduledFuture<?>> tareasTemporizadas = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Queue<String>> colasFO = new ConcurrentHashMap<>();

    private static final String TOPIC_PETICIONES_FO = "peticiones_riego";
    private static final String TOPIC_ORDENES_ENGINE = "ordenes-riego";
    private static final String TOPIC_RESPUESTAS_FO = "respuestas_riego";
    private static final String TOPIC_TELEMETRIA = "telemetria-estaciones";

    // Caché en memoria de caudal y volumen por estación
    private static final ConcurrentHashMap<String, Double> caudalActual = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Double> volumenActual = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso incorrecto de WM_Central.");
            System.out.println("Sintaxis: java WM_Central <puerto_central> <ip_kafka> <puerto_kafka> [puerto_web]");
            System.exit(1);
        }

        puertoEscucha = Integer.parseInt(args[0]);
        ipKafka = args[1];
        puertoKafka = Integer.parseInt(args[2]);

        System.out.println("==================================================");
        System.out.println("      INICIANDO WATER MANAGEMENT CENTRAL          ");
        System.out.println("==================================================");

        // 1. Inicializar y resetear estados en base de datos
        if (GestorCentral.getInstance().cargarDatos()) {
            System.out.println("[BD] Base de datos conectada e inicializada correctamente.");
        } else {
            System.err.println("[BD] Error crítico al inicializar la base de datos.");
            System.exit(1);
        }

        // 2. Registrar hook para cierre ordenado
        Runtime.getRuntime().addShutdownHook(new Thread(WM_Central::detener));

        // 3. Iniciar Productor y Consumidores de Kafka
        iniciarProductorKafka();
        iniciarConsumidorKafkaFO();
        iniciarConsumidorTelemetria();

        // 4. Iniciar Servidor de Sockets TCP en segundo plano
        Thread hiloSockets = new Thread(() -> iniciarServidorSockets(puertoEscucha), "Hilo-ServidorSockets");
        hiloSockets.setDaemon(true);
        hiloSockets.start();

        // 5. Iniciar Servidor Web Dashboard (HTTP)
        int puertoWeb = (args.length >= 4) ? Integer.parseInt(args[3]) : 8081;
        ServidorWebCentral.iniciar(puertoWeb, ipKafka, puertoKafka);

        // 6. Menú interactivo por consola en hilo principal
        Scanner scanner = new Scanner(System.in);
        mostrarMenu();

        while (ejecutando) {
            if (scanner.hasNextLine()) {
                System.out.print("\n[CENTRAL] Comandante > ");
                String comando = scanner.nextLine().trim();
                if (comando.equalsIgnoreCase("1") || comando.equalsIgnoreCase("estaciones")) {
                    mostrarEstadoEstaciones();
                } else if (comando.equalsIgnoreCase("2") || comando.equalsIgnoreCase("regar")) {
                    System.out.print("ID de estación (ej. WS_01): ");
                    String idWS = scanner.nextLine().trim();
                    System.out.print("Duración en segundos (0 = indefinido): ");
                    String durStr = scanner.nextLine().trim();
                    procesarPeticionFO("START_RIEGO#" + idWS + "#CENTRAL#" + durStr);
                } else if (comando.equalsIgnoreCase("3") || comando.equalsIgnoreCase("bloquear")) {
                    System.out.print("ID de estación a bloquear (Fuera de Servicio): ");
                    String idWS = scanner.nextLine().trim();
                    bloquearEstacion(idWS);
                } else if (comando.equalsIgnoreCase("4") || comando.equalsIgnoreCase("activar")) {
                    System.out.print("ID de estación a activar (Disponible): ");
                    String idWS = scanner.nextLine().trim();
                    activarEstacion(idWS);
                } else if (comando.equalsIgnoreCase("5") || comando.equalsIgnoreCase("menu") || comando.equalsIgnoreCase("ayuda")) {
                    mostrarMenu();
                } else if (comando.equalsIgnoreCase("6") || comando.equalsIgnoreCase("limpiar")) {
                    System.out.print("\033[H\033[2J");
                    System.out.flush();
                } else if (comando.equalsIgnoreCase("7") || comando.equalsIgnoreCase("salir") || comando.equalsIgnoreCase("exit")) {
                    System.out.println("[CENTRAL] Apagando Central de Gestión de Agua...");
                    ejecutando = false;
                    break;
                } else if (!comando.isEmpty()) {
                    System.out.println("Comando desconocido. Escribe 'menu' para ver las opciones disponibles.");
                }
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        scanner.close();
        detener();
        System.out.println("[CENTRAL] Central detenida.");
    }

    public static void iniciarServidorSockets(int puerto) {
        try {
            skServidor = new ServerSocket(puerto);
            System.out.println("[SOCKETS] Servidor escuchando en el puerto " + puerto + " para peticiones de clientes (WS_M, WM_FO).");

            while (ejecutando && !skServidor.isClosed()) {
                try {
                    Socket skCliente = skServidor.accept();
                    Thread t = new HiloServidor(skCliente);
                    t.start();
                } catch (SocketException se) {
                    if (!ejecutando) break;
                    System.err.println("[SOCKETS] Socket del servidor cerrado.");
                } catch (Exception e) {
                    System.err.println("[SOCKETS] Error al aceptar cliente: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("[SOCKETS] Error fatal al iniciar ServerSocket en puerto " + puerto + ": " + e.getMessage());
        }
    }

    private static void iniciarProductorKafka() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try {
            productorKafka = new KafkaProducer<>(props);
            System.out.println("[KAFKA] Productor de Central conectado a " + ipKafka + ":" + puertoKafka);
        } catch (Exception e) {
            System.err.println("[KAFKA] Error al conectar el productor: " + e.getMessage());
        }
    }

    private static void iniciarConsumidorKafkaFO() {
        Thread hiloConsumidor = new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-central-peticiones");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            try {
                consumidorFO = new KafkaConsumer<>(props);
                consumidorFO.subscribe(Collections.singletonList(TOPIC_PETICIONES_FO));
                System.out.println("[KAFKA] Central suscrita al topic '" + TOPIC_PETICIONES_FO + "'");

                while (ejecutando) {
                    ConsumerRecords<String, String> records = consumidorFO.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        String mensaje = record.value();
                        if (mensaje == null || mensaje.trim().isEmpty()) {
                            continue;
                        }
                        String[] ordenes = mensaje.split(";");
                        if (ordenes.length > 1) {
                            encolarDoc(ordenes, record.key());
                        } else {
                            procesarPeticionFO(ordenes[0].trim());
                        }
                    }
                }
            } catch (Exception e) {
                if (ejecutando) {
                    System.err.println("[KAFKA] Error en consumidor de peticiones FO: " + e.getMessage());
                }
            } finally {
                if (consumidorFO != null) {
                    try { consumidorFO.close(); } catch (Exception ignored) {}
                }
            }
        }, "Hilo-ConsumidorKafkaFO");
        hiloConsumidor.setDaemon(true);
        hiloConsumidor.start();
    }

    private static void encolarDoc(String[] ordenes, String key) {
        Queue<String> cola = new LinkedList<>();
        String idFO = key;

        for (String p : ordenes) {
            String peticion = p.trim();
            if (!peticion.isEmpty()) {
                cola.add(peticion);
                if (idFO == null && peticion.split("#").length >= 3) {
                    idFO = peticion.split("#")[2].trim();
                }
            }
        }

        if (idFO != null && !cola.isEmpty()) {
            colasFO.put(idFO, cola);
            siguienteTareaDoc(idFO);
        }
    }

    private static void siguienteTareaDoc(String idFO) {
        Queue<String> cola = colasFO.get(idFO);
        if (cola != null && !cola.isEmpty()) {
            String peticion = cola.poll();
            procesarPeticionFO(peticion);
        } else {
            colasFO.remove(idFO);
        }
    }

    private static void espera(String idFO) {
        if (idFO != null && colasFO.containsKey(idFO) && !colasFO.get(idFO).isEmpty()) {
            temporizadorRiego.schedule(() -> {
                siguienteTareaDoc(idFO);
            }, 4, TimeUnit.SECONDS);
        }
    }

    private static void iniciarConsumidorTelemetria() {
        Thread hiloTelemetria = new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-central-telemetria");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            try {
                consumidorTelemetria = new KafkaConsumer<>(props);
                consumidorTelemetria.subscribe(Collections.singletonList(TOPIC_TELEMETRIA));
                System.out.println("[KAFKA] Central suscrita al topic '" + TOPIC_TELEMETRIA + "'");

                while (ejecutando) {
                    ConsumerRecords<String, String> records = consumidorTelemetria.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        procesarTramaTelemetria(record.value());
                    }
                }
            } catch (Exception e) {
                if (ejecutando) {
                    System.err.println("[KAFKA] Error en consumidor de telemetría: " + e.getMessage());
                }
            } finally {
                if (consumidorTelemetria != null) {
                    try { consumidorTelemetria.close(); } catch (Exception ignored) {}
                }
            }
        }, "Hilo-ConsumidorTelemetria");
        hiloTelemetria.setDaemon(true);
        hiloTelemetria.start();
    }

    private static void procesarTramaTelemetria(String trama) {
        try {
            String[] partes = trama.split("#");
            if (partes.length >= 4 && partes[0].equals("TELEMETRIA")) {
                String idWS = partes[1].trim();
                double caudal = Double.parseDouble(partes[2].trim());
                double volAcumulado = Double.parseDouble(partes[3].trim());
                long timestamp = (partes.length >= 5) ? Long.parseLong(partes[4].trim()) : System.currentTimeMillis();

                caudalActual.put(idWS, caudal);
                volumenActual.put(idWS, volAcumulado);

                ServidorWebCentral.actualizarTelemetria(idWS, caudal, volAcumulado, timestamp);

                System.out.printf("\n[TELEMETRÍA KAFKA] Estación: %-7s | Caudal: %6.2f L/s | Consumo Total: %8.2f L\n[CENTRAL] Comandante > ",
                        idWS, caudal, volAcumulado);
            }
        } catch (Exception e) {
            System.err.println("[TELEMETRÍA] Error al parsear trama: " + trama);
        }
    }

    private static void procesarPeticionFO(String mensaje) {
        if (mensaje == null) return;
        String[] partes = mensaje.split("#");
        if (partes.length < 2) return;

        String accion = partes[0].trim();
        String idWS = partes[1].trim();

        if (accion.equals("START_RIEGO") && partes.length == 4) {
            String idFO = partes[2].trim();
            try {
                int duracion = Integer.parseInt(partes[3].trim());

                String valido = GestorCentral.getInstance().procesarPeticion(mensaje);
                if (valido.equals("<ACK>")) {
                    volumenActual.put(idWS, 0.0);
                    enviarEngine("INICIAR_RIEGO#" + idWS, idWS);
                    enviarFO(idWS, idFO, "ACK#" + idWS);
                    ServidorWebCentral.registrarEvento("Riego autorizado en " + idWS + " para operario " + idFO + " (" + duracion + "s)");

                    if (duracion > 0) {
                        tiempoRiego(idWS, idFO, duracion);
                    }
                } else {
                    System.out.println("\n[CENTRAL] Riego rechazado para " + idWS + " (solicitado por " + idFO + ")");
                    ServidorWebCentral.registrarEvento("Riego denegado en " + idWS + " para operario " + idFO);
                    enviarFO(idWS, idFO, "NACK#" + idWS);
                    espera(idFO);
                }
            } catch (NumberFormatException e) {
                System.out.println("[CENTRAL] Error en el formato de la duración.");
                espera(idFO);
            }
        } else if (accion.equals("STOP_RIEGO") && partes.length >= 2) {
            String idFO = (partes.length >= 3) ? partes[2].trim() : GestorCentral.getInstance().obtenerOperario(idWS);
            if (idFO == null) idFO = "DESCONOCIDO";

            cancelarTiempo(idWS);
            GestorCentral.getInstance().procesarPeticion(mensaje);
            enviarEngine("PARAR_RIEGO#" + idWS, idWS);
            double litrosTotales = volumenActual.getOrDefault(idWS, 0.0);
            caudalActual.put(idWS, 0.0);
            ServidorWebCentral.actualizarTelemetria(idWS, 0.0, litrosTotales, System.currentTimeMillis());
            ServidorWebCentral.registrarEvento("Riego finalizado en " + idWS + " (Total: " + litrosTotales + " L)");
            enviarFO(idWS, idFO, "FIN_RIEGO#" + idWS + "#" + litrosTotales);
            espera(idFO);
        } else if (accion.equals("LISTAR") && partes.length >= 2) {
            String idFO = partes[1].trim();
            List<String> estaciones = GestorCentral.getInstance().obtenerEstaciones();
            String payload = String.join("|", estaciones);
            enviarFO("CENTRAL", idFO, "LISTA#" + payload);
        }
    }

    private static void tiempoRiego(String idWS, String idFO, int duracion) {
        cancelarTiempo(idWS);

        ScheduledFuture<?> tarea = temporizadorRiego.schedule(() -> {
            GestorCentral.getInstance().finalizarRiego(idWS);
            enviarEngine("PARAR_RIEGO#" + idWS, idWS);
            double litrosTotales = volumenActual.getOrDefault(idWS, 0.0);
            caudalActual.put(idWS, 0.0);
            ServidorWebCentral.actualizarTelemetria(idWS, 0.0, litrosTotales, System.currentTimeMillis());
            ServidorWebCentral.registrarEvento("Fin temporizado de riego en " + idWS + " (Total: " + litrosTotales + " L)");
            enviarFO(idWS, idFO, "FIN_RIEGO#" + idWS + "#" + litrosTotales);
            tareasTemporizadas.remove(idWS);
            espera(idFO);
        }, duracion, TimeUnit.SECONDS);

        tareasTemporizadas.put(idWS, tarea);
    }

    private static void cancelarTiempo(String idWS) {
        ScheduledFuture<?> tarea = tareasTemporizadas.remove(idWS);
        if (tarea != null && !tarea.isDone()) {
            tarea.cancel(false);
        }
    }

    public static void bloquearEstacion(String idWS) {
        String res = GestorCentral.getInstance().procesarPeticion("BLOQUEAR#" + idWS);
        if (res.startsWith("<ACK>")) {
            cancelarTiempo(idWS);
            enviarEngine("PARAR_RIEGO#" + idWS, idWS);
            caudalActual.put(idWS, 0.0);
            ServidorWebCentral.registrarEvento("Central: Estación " + idWS + " BLOQUEADA (Fuera de Servicio)");
            System.out.println("[CENTRAL] Estación " + idWS + " puesta en FUERA DE SERVICIO.");
        } else {
            System.out.println("[CENTRAL] No se pudo bloquear la estación " + idWS + ".");
        }
    }

    public static void activarEstacion(String idWS) {
        String res = GestorCentral.getInstance().procesarPeticion("ACTIVAR#" + idWS);
        if (res.startsWith("<ACK>")) {
            ServidorWebCentral.registrarEvento("Central: Estación " + idWS + " ACTIVADA (Disponible)");
            System.out.println("[CENTRAL] Estación " + idWS + " activada (DISPONIBLE).");
        } else {
            System.out.println("[CENTRAL] No se pudo activar la estación " + idWS + ".");
        }
    }

    public static void enviarEngine(String com, String idWS) {
        if (productorKafka == null) return;
        ProducerRecord<String, String> ticket = new ProducerRecord<>(TOPIC_ORDENES_ENGINE, idWS, com);
        productorKafka.send(ticket, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("[KAFKA -> Engine] Enviado " + com + " a topic " + TOPIC_ORDENES_ENGINE);
            } else {
                System.out.println("[KAFKA] ERROR al enviar orden al Engine: " + exception.getMessage());
            }
        });
    }

    public static void enviarFO(String idWS, String idFO, String res) {
        if (productorKafka == null) return;
        ProducerRecord<String, String> ticket = new ProducerRecord<>(TOPIC_RESPUESTAS_FO, idFO, res);
        productorKafka.send(ticket, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("[KAFKA -> FO (" + idFO + ")] Respuesta enviada: " + res);
            }
        });
    }

    public static void cortarAguaFuga(String idWS) {
        cortarAguaFuga(idWS, null);
    }

    public static void cortarAguaFuga(String idWS, String operario) {
        System.out.println("\n¡ALERTA! Fuga detectada en " + idWS + " durante riego. Cortando suministro inmediatamente...");
        String idFO = (operario != null && !operario.isEmpty()) ? operario : GestorCentral.getInstance().obtenerOperario(idWS);
        cancelarTiempo(idWS);
        enviarEngine("PARAR_RIEGO#" + idWS, idWS);
        caudalActual.put(idWS, 0.0);
        ServidorWebCentral.registrarEvento("🚨 ¡ALERTA! Fuga en " + idWS + ". Riego cortado automáticamente.");
        if (idFO != null) {
            enviarFO(idWS, idFO, "FUGA#" + idWS);
            espera(idFO);
        }
    }

    private static void mostrarEstadoEstaciones() {
        List<String> estaciones = GestorCentral.getInstance().obtenerEstaciones();
        System.out.println("\n==========================================================================================");
        System.out.println("                         ESTADO DE LAS ESTACIONES DE RIEGO                                ");
        System.out.println("==========================================================================================");
        System.out.printf("%-8s | %-18s | %-24s | %-10s | %-10s\n", "ID_WS", "ESTADO", "UBICACIÓN", "OPERADOR", "VOLUMEN(L)");
        System.out.println("------------------------------------------------------------------------------------------");

        if (estaciones.isEmpty()) {
            System.out.println("  (No hay estaciones registradas todavía en la base de datos)");
        } else {
            for (String info : estaciones) {
                String[] partes = info.split("#");
                String id = partes.length > 0 ? partes[0] : "-";
                String estado = partes.length > 1 ? partes[1] : "-";
                String ubicacion = partes.length > 2 ? partes[2] : "-";
                String operador = partes.length > 3 ? partes[3] : "-";
                double vol = volumenActual.getOrDefault(id, 0.0);

                System.out.printf("%-8s | %-18s | %-24s | %-10s | %8.2f L\n", id, estado, ubicacion, operador, vol);
            }
        }
        System.out.println("==========================================================================================");
    }

    private static void mostrarMenu() {
        System.out.println("\n--- COMANDOS DISPONIBLES EN CENTRAL ---");
        System.out.println("  1 / estaciones  : Listar estaciones y sus estados actuales en BD");
        System.out.println("  2 / regar       : Iniciar riego en una estación desde Central");
        System.out.println("  3 / bloquear    : Bloquear estación (poner Fuera de Servicio)");
        System.out.println("  4 / activar     : Activar estación (volver a Disponible)");
        System.out.println("  5 / menu        : Mostrar este menú de ayuda");
        System.out.println("  6 / limpiar     : Limpiar consola");
        System.out.println("  7 / salir       : Detener la Central y salir");
        System.out.println("  🌐 Dashboard Web: http://localhost:8081/");
        System.out.println("---------------------------------------");
    }

    public static void detener() {
        ejecutando = false;
        temporizadorRiego.shutdownNow();
        ServidorWebCentral.detener();
        if (productorKafka != null) {
            try {
                productorKafka.flush();
                productorKafka.close();
            } catch (Exception ignored) {}
        }
        if (skServidor != null && !skServidor.isClosed()) {
            try {
                skServidor.close();
            } catch (IOException ignored) {}
        }
        if (consumidorTelemetria != null) {
            try { consumidorTelemetria.wakeup(); } catch (Exception ignored) {}
        }
        if (consumidorFO != null) {
            try { consumidorFO.wakeup(); } catch (Exception ignored) {}
        }
    }
}
