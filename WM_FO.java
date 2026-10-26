import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

public class WM_FO {
    private static String ipKafka;
    private static String puertoKafka;
    private static String idFO;
    private static KafkaProducer<String, String> productor;

    private static final String TOPIC_PETICIONES_FO = "peticiones_riego";
    private static final String TOPIC_RESPUESTAS_FO = "respuestas_riego";

    // Para sincronizar la espera hasta que termine cada riego o llegue un rechazo/fuga
    private static volatile CompletableFuture<String> esperaRespuesta;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("=================================================================");
            System.out.println("  Uso de WM_FO (Field Operator):");
            System.out.println("  1. Modo Consola:   java WM_FO <ip_kafka> <puerto_kafka> <idFO>");
            System.out.println("  2. Modo Fichero:   java WM_FO <ip_kafka> <puerto_kafka> <idFO> <ruta_fichero>");
            System.out.println("  3. Modo Directo:   java WM_FO <ip_kafka> <puerto_kafka> <idFO> <idWS> <duracion>");
            System.out.println("=================================================================");
            System.exit(1);
        }

        ipKafka = args[0];
        puertoKafka = args[1];
        idFO = args[2];

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        productor = new KafkaProducer<>(props);

        // Asegurar cierre limpio al terminar
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nCerrando productor de Kafka de forma segura...");
            if (productor != null) {
                productor.close();
            }
        }));

        iniciarConsumidor();

        // Pequeña pausa inicial para asegurar que el consumidor de Kafka esté suscrito
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        if (args.length == 4) {
            ejecutarFichero(args[3]);
        } else if (args.length >= 5) {
            ejecutarTarea(args[3].trim(), args[4].trim());
        } else {
            ejecutarInteractivo();
        }
    }

    public static void ejecutarFichero(String dirDoc) {
        File f = new File(dirDoc);
        if (!f.exists()) {
            System.out.println("ERROR. No se ha encontrado el archivo " + dirDoc);
            return;
        }

        System.out.println("[WM_FO] Procesando fichero de órdenes: " + dirDoc);
        try (Scanner sc = new Scanner(f)) {
            int numlinea = 0;

            while (sc.hasNextLine()) {
                numlinea++;
                String linea = sc.nextLine().trim();
                if (linea.isEmpty() || linea.startsWith("#")) {
                    continue;
                }

                if (linea.toUpperCase().startsWith("STOP#")) {
                    String[] partes = linea.split("#");
                    if (partes.length == 2) {
                        stopRiego(partes[1].trim());
                    }
                } else {
                    String[] partes = linea.split("[,#\\s]+");
                    if (partes.length >= 2) {
                        String idWS = partes[0].trim();
                        String duracion = partes[1].trim();
                        if (numeroValido(duracion)) {
                            System.out.println("\n=== Ejecutando orden #" + numlinea + " del fichero (" + idWS + " - " + duracion + "s) ===");
                            ejecutarTarea(idWS, duracion);
                        } else {
                            System.out.println("AVISO. Duración inválida en la línea: " + numlinea);
                        }
                    } else if (partes.length == 1) {
                        ejecutarTarea(partes[0].trim(), "0");
                    }
                }
            }
            System.out.println("\n[WM_FO] Fin del procesamiento del fichero.");
        } catch (FileNotFoundException e) {
            System.out.println("ERROR. No se ha encontrado el archivo " + dirDoc);
        } catch (Exception e) {
            System.out.println("ERROR durante la ejecución del fichero: " + e.getMessage());
        }
    }

    public static void ejecutarTarea(String idWS, String duracion) {
        String mensaje = "START_RIEGO#" + idWS + "#" + idFO + "#" + duracion;
        enviarKafka(mensaje, idWS);
    }

    public static void ejecutarInteractivo() {
        Scanner consola = new Scanner(System.in);
        System.out.println("=== PANEL FIELD OPERATOR [" + idFO + "] ===");
        System.out.println("Modo interactivo. Escriba 'AYUDA' para ver comandos.");

        while (true) {
            System.out.print("[" + idFO + "] => ");
            if (!consola.hasNextLine()) {
                try { Thread.sleep(2000); } catch (InterruptedException e) { break; }
                continue;
            }
            String linea = consola.nextLine().trim();

            if (linea.isEmpty()) {
                continue;
            }

            if (linea.equalsIgnoreCase("EXIT") || linea.equalsIgnoreCase("SALIR") || linea.equals("3")) {
                System.out.println("Saliendo...");
                break;
            }

            if (linea.equalsIgnoreCase("AYUDA") || linea.equalsIgnoreCase("HELP")) {
                System.out.println("\n--- COMANDOS DISPONIBLES ---");
                System.out.println(" 1. Riego con duración : <idWS>#<segundos>   (Ej: WS_01#10)");
                System.out.println(" 2. Riego indefinido   : <idWS>              (Ej: WS_01)");
                System.out.println(" 3. Parar riego manual : STOP#<idWS>         (Ej: STOP#WS_01)");
                System.out.println(" 4. Cargar fichero     : FILE#<ruta>         (Ej: FILE#tareas.txt)");
                System.out.println(" 5. Salir del programa : SALIR o EXIT");
                continue;
            }

            if (linea.toUpperCase().startsWith("FILE#") || linea.toUpperCase().startsWith("CARGAR#")) {
                String[] partes = linea.split("#");
                if (partes.length == 2 && !partes[1].trim().isEmpty()) {
                    ejecutarFichero(partes[1].trim());
                } else {
                    System.out.println("[AVISO] Formato incorrecto. Uso: FILE#ruta_archivo.txt");
                }
                continue;
            }

            if (linea.toUpperCase().startsWith("STOP#")) {
                String[] partes = linea.split("#");
                if (partes.length == 2 && !partes[1].trim().isEmpty()) {
                    stopRiego(partes[1].trim());
                } else {
                    System.out.println("[AVISO] Formato incorrecto. Uso: STOP#idWS");
                }
                continue;
            }

            String[] partes = linea.split("#");
            if (partes.length == 2) {
                String idWS = partes[0].trim();
                String duracion = partes[1].trim();

                if (numeroValido(duracion)) {
                    ejecutarTarea(idWS, duracion);
                } else {
                    System.out.println("ERROR. La duración debe ser un número entero mayor o igual a 0.");
                }
            } else if (partes.length == 1) {
                String idWS = partes[0].trim();
                ejecutarTarea(idWS, "0");
            } else {
                System.out.println("ERROR. Formato no válido. Escriba 'AYUDA' para ver los comandos.");
            }
        }
        consola.close();
    }

    public static void stopRiego(String idWS) {
        String mensaje = "STOP_RIEGO#" + idWS + "#" + idFO;
        enviarKafka(mensaje, idWS);
    }

    private static void enviarKafka(String mensaje, String idWS) {
        int duracion = 15;
        ProducerRecord<String, String> ticket = new ProducerRecord<>(TOPIC_PETICIONES_FO, idWS, mensaje);
        esperaRespuesta = new CompletableFuture<>();
        try {
            String[] partes = mensaje.split("#");
            if (mensaje.startsWith("START_RIEGO") && partes.length == 4) {
                try {
                    duracion = Integer.parseInt(partes[3].trim());
                } catch (NumberFormatException ignored) {}
            }
            productor.send(ticket).get();
            System.out.println("[WM_FO -> Kafka] Petición enviada a Central: " + mensaje);
        } catch (Exception e) {
            System.out.println("[ERROR KAFKA] Fallo al entregar: " + e.getMessage());
        }
        try {
            if (duracion > 0) {
                esperaRespuesta.get(duracion + 5, TimeUnit.SECONDS);
            } else {
                esperaRespuesta.get(10, TimeUnit.SECONDS);
            }
            System.out.println("[WM_FO] Esperando 4 segundos reglamentarios antes de la siguiente operación...");
            Thread.sleep(4000);
        } catch (Exception e) {
            System.out.println("AVISO. Espera de respuesta finalizada o interrumpida.");
        }

        System.out.println("Canal disponible:");
    }

    public static boolean numeroValido(String num) {
        try {
            int duracion = Integer.parseInt(num);
            return duracion >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static void iniciarConsumidor() {
        Thread hiloConsumidor = new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-fo-" + idFO);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props)) {
                consumidor.subscribe(Collections.singletonList(TOPIC_RESPUESTAS_FO));

                while (true) {
                    ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        if (record.key() == null || !record.key().equals(idFO)) {
                            continue;
                        }
                        String mensaje = record.value();
                        String pantalla = gestionarMensaje(mensaje);
                        System.out.println("\n[CENTRAL => " + idFO + "] " + pantalla);

                        if (esperaRespuesta != null && !esperaRespuesta.isDone()) {
                            if (mensaje.startsWith("FIN_RIEGO") || mensaje.startsWith("NACK") || mensaje.startsWith("FUGA")) {
                                esperaRespuesta.complete(mensaje);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("ERROR en el hilo consumidor de FO: " + e.getMessage());
            }
        });

        hiloConsumidor.setDaemon(true);
        hiloConsumidor.start();
    }

    public static String gestionarMensaje(String msg) {
        String[] partes = msg.split("#");
        String res = partes[0].trim();
        String idWS = partes.length > 1 ? partes[1].trim() : "";
        String litros = partes.length >= 3 ? partes[2].trim() : "0.0";

        if (res.equals("ACK")) {
            return "Riego AUTORIZADO en estación " + idWS + ". Abriendo electroválvula...";
        } else if (res.equals("NACK")) {
            return "Riego DENEGADO en estación " + idWS + " (No disponible, ocupada o en avería).";
        } else if (res.equals("FUGA")) {
            return "¡ALERTA! FUGA detectada en estación " + idWS + ". Suministro cancelado inmediatamente.";
        } else {
            return "FIN DE RIEGO en estación " + idWS + " | Resumen volumen total consumido: " + litros + " litros.";
        }
    }
}