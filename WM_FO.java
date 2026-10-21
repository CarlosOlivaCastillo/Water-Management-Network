import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

class WM_FO{
    private static String ipKafka;
    private static String puertoKafka;
    private static String idFO;
    private static KafkaProducer<String, String> productor;

	private static final String TOPIC_RESPUESTAS_FO = "respuestas_riego";
    // PARA CONTROLAR LA SINCRONIZACIÓN
    private static volatile CompletableFuture<String> esperaRespuesta;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Debe indicar la IP de Kafka, el puerto de Kafka y el ID del FO");
            System.out.println("$./WM_FO <ip_kafka> <puerto_kafka> <idFO>\n");
            System.exit (1);
        }
        ipKafka = args[0];
        puertoKafka = args[1];
        idFO = args[2];

        Properties props = new Properties();
        props.put("bootstrap.servers", ipKafka + ":" + puertoKafka);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        productor = new KafkaProducer<>(props);

        // PARA ASEGURARNOS DE CERRAR LA CONEXIÓN JUSTO ANTES DE CERRAR
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nCerrando productor de Kafka de forma segura...");
            productor.close();
        }));

        iniciarConsumidor();
        ejecutarInteractivo();
    }




    public static void ejecutarFichero(String dirDoc){
        try (Scanner sc = new Scanner(new File(dirDoc))) {
            int numlinea = 0;

            while(sc.hasNextLine()){
                numlinea++;
                String linea = sc.nextLine().trim();
                if(linea.trim().isEmpty()){
                    continue;
                }

                if(linea.toUpperCase().startsWith("STOP#")){
                    String[] partes = linea.split("#");
                    if(partes.length == 2){
                        stopRiego(partes[1]);
                    }
                }else{
                    String[] partes = linea.split("#");
                    if(partes.length == 2){
                        String idWS = partes[0].trim();
                        String duracion = partes[1].trim();
                        if(numeroValido(duracion)){
                            ejecutarTarea(idWS, duracion);
                        }else{
                            System.out.println("AVISO. Duración inválida en la linea: " + numlinea);
                        }
                    }else if(partes.length == 1){
                        ejecutarTarea(partes[0].trim(),"0");
                    }
                }
            }
        }catch(FileNotFoundException e){
            System.out.println("ERROR. No se ha encontrado el archivo " + dirDoc);
        }catch(Exception e){
            System.out.println("ERROR durante la ejecución del fichero");
        }
    }



    public static void ejecutarTarea(String idWS, String duracion){
        String mensaje = "START_RIEGO#" + idWS + "#" + idFO + "#" + duracion;
        enviarKafka(mensaje, idWS);
    }



    public static void ejecutarInteractivo(){
        Scanner consola = new Scanner(System.in);
        System.out.println("Modo interactivo. Escriba ayuda para ver comandos");


        while(true){
            System.out.println("=>");
            String linea = consola.nextLine();

            if(linea.trim().isEmpty()){
                continue;
            }

            if(linea.toUpperCase().equals("EXIT") || linea.toUpperCase().equals("SALIR")){
                System.out.println("Saliendo...");
                break;
            }

            if(linea.toUpperCase().equals("AYUDA") || linea.toUpperCase().equals("HELP")){
                System.out.println("\n--- COMANDOS DISPONIBLES ---");
                System.out.println(" 1. Riego con duracion : <idWS>#<minutos>    (Ej: WS_01#15)");
                System.out.println(" 2. Riego indefinido   : <idWS>             (Ej: WS_01)");
                System.out.println(" 3. Parar riego manual : STOP#<idWS>        (Ej: STOP#WS_01)");
                System.out.println(" 4. Cargar fichero     : FILE#<ruta>        (Ej: FILE#tareas.txt)");
                System.out.println(" 5. Salir del programa : SALIR o EXIT");
                continue;
            }

            // FILE#[ruta-fichero]
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
            if(partes.length == 2){
                String idWS = partes[0].trim();
                String duracion = partes[1].trim();

                if(numeroValido(duracion)){
                    ejecutarTarea(idWS, duracion);
                }else{
                    System.out.println("ERROR. La duración no puede ser un número negativo");
                }
            }else if(partes.length == 1){
                String idWS = partes[0].trim();
                ejecutarTarea(idWS, "0");
            }else{
                System.out.println("ERROR. Formato no válido. Escriba 'AYUDA' para ver los comandos");
            }
            
        }
        consola.close();
    }

    public static void stopRiego(String idWS){
        String mensaje = "STOP_RIEGO#" + idWS + "#" + idFO;
        enviarKafka(mensaje, idWS);
    } 


    private static void enviarKafka(String mensaje, String idWS){
        int duracion = 15;
        ProducerRecord<String, String> ticket = new ProducerRecord<String,String>("peticiones_riego",idWS, mensaje);
        esperaRespuesta = new CompletableFuture<>();
        try {
            String[] partes = mensaje.split("#");
            if(mensaje.startsWith("START_RIEGO") && partes.length == 4){
                try{
                    duracion = Integer.parseInt(partes[3].trim());
                }catch(NumberFormatException e){
                }
            }
            // .get() bloqueaa la ejecución hasta recibir la respuesta
            productor.send(ticket).get();   
        } catch (Exception e) {
            System.out.println("[ERROR KAFKA] Fallo al entregar: " + e.getMessage());
        }
        try{
            esperaRespuesta.get(duracion + 1, TimeUnit.SECONDS);
            Thread.sleep(4000);
        }catch(Exception e){
            System.out.println("AVISO. Pausa interrumpida");
        }

        System.out.println("Canal disponible:");
    }


    public static boolean numeroValido(String num){
        try{
            int duracion = Integer.parseInt(num);
            return duracion >= 0;
        }catch(NumberFormatException e){
            return false;
        }
    }



    public static void iniciarConsumidor(){
        Thread hiloConsumidor = new Thread(() -> {
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-fo-" + idFO);
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

			KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props);
			consumidor.subscribe(Collections.singletonList(TOPIC_RESPUESTAS_FO));

			try{
				while(true){
					// LEE DURANTE 1 SEGUNDO, SI LLEGA LO TRATA AL INSTANTE SI NO DEVUELVE NULL Y VUELVE AL BUCLE
					ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(1000));
					for(ConsumerRecord<String, String> record : records){
						if(record.key() == null || !record.key().equals(idFO)){
                            continue;
                        }
                        String mensaje = record.value();
                        String pantalla = gestionarMensaje(mensaje);
                        System.out.println("\n[CENTRAL] => " + pantalla);
                        System.out.println("=>: ");

                        if(esperaRespuesta != null && !esperaRespuesta.isDone()){
                            if (mensaje.startsWith("FIN_RIEGO") || mensaje.startsWith("NACK") || mensaje.startsWith("FUGA")) {
                                esperaRespuesta.complete(mensaje); // enviarKafka se despierta
                            }
                        }
					}
				}
			}catch(Exception e){
				System.out.println("ERROR en el hilo consumidor: " + e.getMessage());
			}finally{
				consumidor.close();
			}
		});
		
        hiloConsumidor.setDaemon(true);
        hiloConsumidor.start();
    }


    public static String gestionarMensaje(String msg){
        String[] partes = msg.split("#");
        String res = partes[0].trim();
        String idWS = partes[1].trim();
        String litros = null;
        if(partes.length == 3){
            litros = partes[2].trim();
        }

        if(res.equals("ACK")){
            return "Riego autorizado en " + idWS;
        }else if(res.equals("NACK")){
            return "Riego denegado en " + idWS;
        }else if(res.equals("FUGA")){
            return "FUGA detectada en " + idWS;
        }else{
            return "Fin de riego, " + litros + "litros en " + idWS;
        }
    } 
}