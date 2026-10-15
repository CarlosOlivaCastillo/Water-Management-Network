import java.io.*;
import java.net.*;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class WM_Central {

    private static int puertoEscucha;
    private static String ipKafka;
    private static int puertoKafka;
    private static ServerSocket skServidor;
    private static volatile boolean ejecutando = true;
    private static KafkaConsumer<String, String> consumidorKafka;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso incorrecto de WM_Central.");
            System.out.println("Sintaxis: java WM_Central <puerto_central> <ip_kafka> <puerto_kafka>");
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

        // 3. Iniciar Servidor de Sockets TCP en segundo plano
        Thread hiloSockets = new Thread(() -> iniciarServidorSockets(puertoEscucha), "Hilo-ServidorSockets");
        hiloSockets.setDaemon(true);
        hiloSockets.start();

        // 4. Iniciar Consumidor de Telemetría Kafka en segundo plano
        Thread hiloKafka = new Thread(() -> iniciarConsumidorKafka(ipKafka, puertoKafka), "Hilo-ConsumidorKafka");
        hiloKafka.setDaemon(true);
        hiloKafka.start();

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
                } else if (comando.equalsIgnoreCase("2") || comando.equalsIgnoreCase("menu") || comando.equalsIgnoreCase("ayuda")) {
                    mostrarMenu();
                } else if (comando.equalsIgnoreCase("3") || comando.equalsIgnoreCase("limpiar")) {
                    System.out.print("\033[H\033[2J");
                    System.out.flush();
                } else if (comando.equalsIgnoreCase("4") || comando.equalsIgnoreCase("salir") || comando.equalsIgnoreCase("exit")) {
                    System.out.println("[CENTRAL] Apagando Central de Gestión de Agua...");
                    ejecutando = false;
                    break;
                } else if (!comando.isEmpty()) {
                    System.out.println("Comando desconocido. Escribe 'menu' para ver las opciones disponibles.");
                }
            } else {
                // Entorno sin terminal interactiva (ej. contenedor Docker en segundo plano)
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

    /**
     * Inicia el bucle de escucha de conexiones Sockets TCP.
     */
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

    /**
     * Inicia el consumidor Kafka para procesar el topic de telemetría en tiempo real.
     */
    public static void iniciarConsumidorKafka(String ipKafka, int puertoKafka) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-monitoreo-telemetria");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try {
            consumidorKafka = new KafkaConsumer<>(props);
            consumidorKafka.subscribe(Collections.singletonList("telemetria-estaciones"));
            System.out.println("[KAFKA] Central suscrita al topic 'telemetria-estaciones' en " + ipKafka + ":" + puertoKafka);

            while (ejecutando) {
                ConsumerRecords<String, String> records = consumidorKafka.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    procesarTramaTelemetria(record.value());
                }
            }
        } catch (Exception e) {
            if (ejecutando) {
                System.err.println("[KAFKA] Aviso: No se pudo conectar al broker Kafka (" + ipKafka + ":" + puertoKafka + "). " + e.getMessage());
            }
        } finally {
            if (consumidorKafka != null) {
                try {
                    consumidorKafka.close();
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Procesa y muestra los datos de telemetría recibidos por Kafka.
     * Formato: TELEMETRIA#<idWS>#<caudal>#<volumenAcumulado>#<timestamp>
     */
    private static void procesarTramaTelemetria(String trama) {
        try {
            String[] partes = trama.split("#");
            if (partes.length >= 5 && partes[0].equals("TELEMETRIA")) {
                String idWS = partes[1];
                double caudal = Double.parseDouble(partes[2]);
                double volAcumulado = Double.parseDouble(partes[3]);
                long timestamp = Long.parseLong(partes[4]);

                // Actualizar métricas para el Dashboard Web
                ServidorWebCentral.actualizarTelemetria(idWS, caudal, volAcumulado, timestamp);

                System.out.printf("\n[TELEMETRÍA KAFKA] Estación: %-7s | Caudal: %6.2f L/s | Consumo Total: %8.2f L | TS: %d\n[CENTRAL] Comandante > ",
                        idWS, caudal, volAcumulado, timestamp);
            }
        } catch (Exception e) {
            System.err.println("[TELEMETRÍA] Error al parsear trama: " + trama);
        }
    }

    /**
     * Muestra una tabla con el estado actual de todas las estaciones registradas en la BD.
     */
    private static void mostrarEstadoEstaciones() {
        List<String> estaciones = GestorCentral.getInstance().obtenerEstaciones();
        System.out.println("\n==========================================================================");
        System.out.println("                 ESTADO DE LAS ESTACIONES DE RIEGO                        ");
        System.out.println("==========================================================================");
        System.out.printf("%-10s | %-14s | %-20s | %-10s\n", "ID_WS", "ESTADO", "UBICACIÓN", "OPERADOR");
        System.out.println("--------------------------------------------------------------------------");

        if (estaciones.isEmpty()) {
            System.out.println("  (No hay estaciones registradas todavía en la base de datos)");
        } else {
            for (String info : estaciones) {
                String[] partes = info.split("#");
                String id = partes.length > 0 ? partes[0] : "-";
                String estado = partes.length > 1 ? partes[1] : "-";
                String ubicacion = partes.length > 2 ? partes[2] : "-";
                String operador = partes.length > 3 ? partes[3] : "-";

                System.out.printf("%-10s | %-14s | %-20s | %-10s\n", id, estado, ubicacion, operador);
            }
        }
        System.out.println("==========================================================================");
    }

    private static void mostrarMenu() {
        System.out.println("\n--- COMANDOS DISPONIBLES EN CENTRAL ---");
        System.out.println("  1 / estaciones  : Listar estaciones y sus estados actuales en BD");
        System.out.println("  2 / menu        : Mostrar este menú de ayuda");
        System.out.println("  3 / limpiar     : Limpiar consola");
        System.out.println("  4 / salir       : Detener la Central y salir");
        System.out.println("  🌐 Dashboard Web: http://localhost:8081/");
        System.out.println("---------------------------------------");
    }

    /**
     * Detiene los servicios de Sockets, Kafka y Web de manera segura.
     */
    public static void detener() {
        ejecutando = false;
        ServidorWebCentral.detener();
        if (skServidor != null && !skServidor.isClosed()) {
            try {
                skServidor.close();
            } catch (IOException ignored) {}
        }
        if (consumidorKafka != null) {
            try {
                consumidorKafka.wakeup();
            } catch (Exception ignored) {}
        }
    }
}
