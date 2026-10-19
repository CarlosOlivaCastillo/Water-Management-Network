import java.io.*;
import java.util.*;
import org.apache.kafka.clients.producer.*;

class WM_FO{
    private static String ipKafka;
    private static String puertoKafka;
    private static String idFO;
    private static KafkaProducer<String, String> productor;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Debe indicar la IP de Kafka, el puerto de Kafka y el ID del FO");
            System.out.println("$./WM_FO <ip_kafka> <puerto_kafka> <idFO>\n");
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
        ProducerRecord<String, String> ticket = new ProducerRecord<String,String>("peticiones_riego",idWS, mensaje);

        try {
            // .get() bloqueaa la ejecución hasta recibir la respuesta
            productor.send(ticket).get();   
        } catch (Exception e) {
            System.out.println("[ERROR KAFKA] Fallo al entregar: " + e.getMessage());
        }

        try{
            Thread.sleep(4000);
        }catch(InterruptedException e){
            System.out.println("AVISO. Pausa interrumpida");
        }

        System.out.println("Canal disponible:");
    }


    public static boolean numeroValido(String num){
        try{
            int duracion = Integer.parseInt(num);
            return duracion > 0;
        }catch(NumberFormatException e){
            return false;
        }
    }
}