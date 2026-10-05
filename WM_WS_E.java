import java.net.*;
import java.io.*;
import java.util.Scanner;
import java.util.Properties;
import java.util.Collections;
import java.util.Locale;
import java.time.Duration;

// Librerías de Kafka
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WM_WS_E {
    private static volatile boolean averiaSimulada = false;
    private static volatile boolean regando = false;
    private static volatile double volumenAcumulado = 0.0;
    private static final double CAUDAL_ESTANDAR = 12.5; // Litros por segundo

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_E <IP_Kafka> <Puerto_Kafka> <IP_Monitor> <Puerto_Monitor> [ID_WS]");
            System.exit(1);
        }

        String ipKafka = args[0];
        String puertoKafka = args[1];
        String ipMonitor = args[2];
        int puertoMonitor = Integer.parseInt(args[3]);
        String idWS = (args.length >= 5) ? args[4] : "WS_01";

        System.out.println("Iniciando Engine para Estación [" + idWS + "]...");

        // 1. Hilo de simulación de averías (Teclado)
        Thread hiloTeclado = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Pulsa ENTER en cualquier momento para simular un KO (Fuga)...");
            scanner.nextLine();
            averiaSimulada = true;
            System.out.println("¡Avería simulada activada! El próximo reporte será KO.");
            scanner.close();
        });
        hiloTeclado.start();

        // 2. Hilo Consumidor de Kafka (Asíncrono)
        Thread hiloKafka = new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-estaciones-" + idWS);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList("ordenes-riego"));
            System.out.println("Engine conectado a Kafka. Escuchando órdenes en 'ordenes-riego'...");

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        String mensaje = record.value();
                        
                        // Si la orden es abrir el agua para esta estación en concreto o global
                        if (mensaje.contains("INICIAR_RIEGO")) {
                            volumenAcumulado = 0.0; // Reiniciar acumulador de la sesión
                            regando = true;
                            System.out.println(">>> ORDEN RECIBIDA: Electroválvula ABIERTA. Comenzando riego.");
                        } else if (mensaje.contains("PARAR_RIEGO")) {
                            regando = false;
                            System.out.println(">>> ORDEN RECIBIDA: Electroválvula CERRADA. Riego detenido.");
                        }
                    }
                }
            } finally {
                consumer.close();
            }
        });
        hiloKafka.start();

        // 3. Hilo Productor de Telemetría (Kafka)
        Thread hiloTelemetria = new Thread(() -> {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                System.out.println("Productor de Telemetría iniciado. Publicando en 'telemetria-estaciones' cuando se active el riego.");
                while (true) {
                    if (regando) {
                        volumenAcumulado += CAUDAL_ESTANDAR;
                        long timestamp = System.currentTimeMillis();
                        String tramaTelemetria = String.format(Locale.US, "TELEMETRIA#%s#%.2f#%.2f#%d",
                                idWS, CAUDAL_ESTANDAR, volumenAcumulado, timestamp);

                        producer.send(new ProducerRecord<>("telemetria-estaciones", idWS, tramaTelemetria));
                        System.out.println("[TELEMETRÍA ENVIADA] " + tramaTelemetria);

                        Thread.sleep(1000);
                    } else {
                        Thread.sleep(500);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("Hilo de telemetría interrumpido.");
            } catch (Exception e) {
                System.err.println("Error en el productor de telemetría: " + e.getMessage());
            }
        });
        hiloTelemetria.start();

        // 4. Bucle principal: Conexión Sockets con el Monitor
        try {
            Socket skMonitor = new Socket(ipMonitor, puertoMonitor);
            DataInputStream flujoEntrada = new DataInputStream(skMonitor.getInputStream());
            DataOutputStream flujoSalida = new DataOutputStream(skMonitor.getOutputStream());
            System.out.println("Conectado al Monitor localmente.");

            while (true) {
                String peticionTrama = flujoEntrada.readUTF();
                String peticion = ProtocoloUtil.desempaquetar(peticionTrama);
                
                if (peticion.equals("STATUS_CHECK")) {
                    String respuesta = averiaSimulada ? "KO" : "OK";
                    String respuestaTrama = ProtocoloUtil.empaquetar(respuesta);
                    flujoSalida.writeUTF(respuestaTrama);
                }
            }
        } catch (Exception e) {
            System.out.println("Error de conexión con el Monitor: " + e.getMessage());
        }
    }
}