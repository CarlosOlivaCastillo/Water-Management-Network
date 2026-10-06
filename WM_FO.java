import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WM_FO {
    private String ipKafka;
    private String puertoKafka;
    private String idFO;
    private String ipCentral;
    private int puertoCentral;
    private KafkaProducer<String, String> producer;

    public WM_FO(String ipKafka, String puertoKafka, String idFO, String ipCentral, int puertoCentral) {
        this.ipKafka = ipKafka;
        this.puertoKafka = puertoKafka;
        this.idFO = idFO;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        inicializarKafkaProducer();
    }

    private void inicializarKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 4000);

        try {
            this.producer = new KafkaProducer<>(props);
            System.out.println("[WM_FO] Productor de Kafka conectado a " + ipKafka + ":" + puertoKafka);
        } catch (Exception e) {
            System.err.println("[WM_FO] Error al conectar con Kafka: " + e.getMessage());
        }
    }

    /**
     * Solicita autorización a Central por Sockets TCP.
     * Retorna true si Central responde <ACK>.
     */
    public boolean solicitarAutorizacion(String idWS, int duracion) {
        System.out.println("[WM_FO] Solicitando autorización a Central (" + ipCentral + ":" + puertoCentral + ") para regar " + idWS + " (" + duracion + "s)...");
        try (Socket skCentral = new Socket(ipCentral, puertoCentral);
             DataOutputStream out = new DataOutputStream(skCentral.getOutputStream());
             DataInputStream in = new DataInputStream(skCentral.getInputStream())) {

            String peticion = "START_RIEGO#" + idWS + "#" + idFO + "#" + duracion;
            String trama = ProtocoloUtil.empaquetar(peticion);
            out.writeUTF(trama);

            String respuesta = in.readUTF();
            System.out.println("[WM_FO] Respuesta de Central: " + respuesta);

            out.writeUTF("exit");
            return "<ACK>".equals(respuesta);

        } catch (Exception e) {
            System.err.println("[WM_FO] Error de comunicación con Central: " + e.getMessage());
            return false;
        }
    }

    /**
     * Notifica a Central el fin del riego por Sockets TCP.
     */
    public boolean notificarFinRiego(String idWS) {
        System.out.println("[WM_FO] Notificando fin de riego a Central para " + idWS + "...");
        try (Socket skCentral = new Socket(ipCentral, puertoCentral);
             DataOutputStream out = new DataOutputStream(skCentral.getOutputStream());
             DataInputStream in = new DataInputStream(skCentral.getInputStream())) {

            String peticion = "STOP_RIEGO#" + idWS;
            String trama = ProtocoloUtil.empaquetar(peticion);
            out.writeUTF(trama);

            String respuesta = in.readUTF();
            System.out.println("[WM_FO] Confirmación fin de riego de Central: " + respuesta);

            out.writeUTF("exit");
            return "<ACK>".equals(respuesta);

        } catch (Exception e) {
            System.err.println("[WM_FO] Error al notificar fin de riego a Central: " + e.getMessage());
            return false;
        }
    }

    /**
     * Publica una orden en el topic de Kafka 'ordenes-riego'.
     */
    public void publicarOrdenKafka(String orden) {
        if (producer == null) {
            System.err.println("[WM_FO] Error: Productor de Kafka no disponible.");
            return;
        }
        try {
            producer.send(new ProducerRecord<>("ordenes-riego", orden), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("[WM_FO -> Kafka] Fallo al entregar mensaje a Kafka: " + exception.getMessage());
                }
            });
            System.out.println("[WM_FO -> Kafka] Publicada orden: " + orden);
        } catch (Exception e) {
            System.err.println("[WM_FO] Error al enviar orden a Kafka: " + e.getMessage());
        }
    }

    /**
     * Ejecuta una tarea de riego completa para una estación dada.
     */
    public void ejecutarRiego(String idWS, int duracion) {
        System.out.println("\n--------------------------------------------------");
        System.out.println("[WM_FO] INICIANDO ORDEN DE RIEGO");
        System.out.println("  Operador: " + idFO);
        System.out.println("  Estación: " + idWS);
        System.out.println("  Duración: " + duracion + " segundos");
        System.out.println("--------------------------------------------------");

        // 1. Solicitar autorización a Central
        boolean autorizada = solicitarAutorizacion(idWS, duracion);
        if (!autorizada) {
            System.out.println("[WM_FO] [DENEGADO] La Central denegó el riego para " + idWS + " (estación ocupada, averiada o inaccesible).");
            return;
        }

        // 2. Enviar orden a Kafka para abrir electroválvula
        String ordenInicio = "INICIAR_RIEGO#" + idWS;
        publicarOrdenKafka(ordenInicio);
        System.out.println("[WM_FO] Electroválvula activada. Regando durante " + duracion + "s...");

        // 3. Temporizador de riego
        try {
            for (int i = duracion; i > 0; i--) {
                System.out.print("\r[WM_FO] Tiempo restante de riego: " + i + "s...   ");
                Thread.sleep(1000);
            }
            System.out.println("\r[WM_FO] Tiempo de riego finalizado.                     ");
        } catch (InterruptedException e) {
            System.out.println("\n[WM_FO] Riego interrumpido.");
        }

        // 4. Enviar orden a Kafka para cerrar electroválvula
        String ordenParar = "PARAR_RIEGO#" + idWS;
        publicarOrdenKafka(ordenParar);

        // 5. Notificar a Central fin de riego
        notificarFinRiego(idWS);
        System.out.println("[WM_FO] [COMPLETADO] Sesión de riego finalizada con éxito.");
    }

    /**
     * Ejecuta órdenes desde un fichero de texto por lotes.
     * Formato por línea: ID_WS DURACION (ej: WS_01 10)
     */
    public void procesarFichero(String rutaFichero) {
        File f = new File(rutaFichero);
        if (!f.exists()) {
            System.err.println("[WM_FO] El fichero especificado no existe: " + rutaFichero);
            return;
        }

        System.out.println("[WM_FO] Procesando fichero de órdenes: " + rutaFichero);
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String linea;
            int numLinea = 0;
            while ((linea = br.readLine()) != null) {
                linea = linea.trim();
                if (linea.isEmpty() || linea.startsWith("#")) continue;

                numLinea++;
                String[] partes = linea.split("[,#\\s]+");
                if (partes.length >= 2) {
                    String idWS = partes[0];
                    try {
                        int duracion = Integer.parseInt(partes[1]);
                        System.out.println("\n=== Ejecutando orden #" + numLinea + " del fichero ===");
                        ejecutarRiego(idWS, duracion);
                    } catch (NumberFormatException nfe) {
                        System.err.println("[WM_FO] Duración inválida en línea " + numLinea + ": " + linea);
                    }
                } else {
                    System.err.println("[WM_FO] Formato de línea inválido en línea " + numLinea + ": " + linea);
                }
            }
            System.out.println("\n[WM_FO] Fin del procesamiento del fichero.");
        } catch (Exception e) {
            System.err.println("[WM_FO] Error al leer el fichero: " + e.getMessage());
        }
    }

    public void cerrar() {
        if (producer != null) {
            producer.close();
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("=================================================================");
            System.out.println("  Uso de WM_FO (Field Operator):");
            System.out.println("  1. Modo Directo:   java WM_FO <ip_kafka> <puerto_kafka> <idFO> <idWS> <duracion>");
            System.out.println("  2. Modo Fichero:   java WM_FO <ip_kafka> <puerto_kafka> <idFO> <ruta_fichero>");
            System.out.println("  3. Modo Consola:   java WM_FO <ip_kafka> <puerto_kafka> <idFO>");
            System.out.println("=================================================================");
            System.exit(1);
        }

        String ipKafka = args[0];
        String puertoKafka = args[1];
        String idFO = args[2];

        // IP y Puerto de Central (por variables de entorno o valores por defecto)
        String ipCentral = System.getenv("IP_CENTRAL") != null ? System.getenv("IP_CENTRAL") : "localhost";
        int puertoCentral = 8080;
        if (System.getenv("PUERTO_CENTRAL") != null) {
            try {
                puertoCentral = Integer.parseInt(System.getenv("PUERTO_CENTRAL"));
            } catch (NumberFormatException ignored) {}
        }

        WM_FO fo = new WM_FO(ipKafka, puertoKafka, idFO, ipCentral, puertoCentral);

        Runtime.getRuntime().addShutdownHook(new Thread(fo::cerrar));

        if (args.length == 4) {
            // Modo fichero
            String rutaFichero = args[3];
            fo.procesarFichero(rutaFichero);
        } else if (args.length >= 5) {
            // Modo directo: idWS duracion
            String idWS = args[3];
            int duracion = 10;
            try {
                duracion = Integer.parseInt(args[4]);
            } catch (NumberFormatException e) {
                System.err.println("Duración no válida, usando 10s por defecto.");
            }
            fo.ejecutarRiego(idWS, duracion);
        } else {
            // Modo interactivo por consola
            Scanner scanner = new Scanner(System.in);
            boolean salir = false;

            while (!salir) {
                System.out.println("\n=== PANEL FIELD OPERATOR [" + idFO + "] ===");
                System.out.println("1. Solicitar y ejecutar riego manual");
                System.out.println("2. Cargar fichero de órdenes por lotes");
                System.out.println("3. Salir");
                System.out.print("Selecciona una opción: ");

                String opc = scanner.nextLine().trim();
                switch (opc) {
                    case "1" -> {
                        System.out.print("ID de la Estación (ej. WS_01): ");
                        String ws = scanner.nextLine().trim();
                        System.out.print("Duración en segundos: ");
                        String durStr = scanner.nextLine().trim();
                        try {
                            int dur = Integer.parseInt(durStr);
                            fo.ejecutarRiego(ws, dur);
                        } catch (NumberFormatException e) {
                            System.out.println("Duración inválida.");
                        }
                    }
                    case "2" -> {
                        System.out.print("Ruta del fichero: ");
                        String ruta = scanner.nextLine().trim();
                        fo.procesarFichero(ruta);
                    }
                    case "3" -> salir = true;
                    default -> System.out.println("Opción no válida.");
                }
            }
            scanner.close();
        }

        fo.cerrar();
    }
}