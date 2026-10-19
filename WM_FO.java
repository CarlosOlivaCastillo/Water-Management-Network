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
            System.out.println("$./WM_FO <ip_kafka> <puerto_kafka> <idFO> <idWS> <duracion> \n");
            System.out.println("$./WM_FO <ip_kafka> <puerto_kafka> <idFO> [documento] \n");
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

        if(args.length == 3){
            ejecutarInteractivo();
        }else if(args.length == 4){
            String dirDoc = args[3];
            ejecutarFichero(dirDoc);
        }else if(args.length == 5){
            String idWS = args[3];
            String duracion = args[4];
            ejecutarTarea(idWS, duracion);
        }
    }




    public static void ejecutarFichero(String dirDoc){
        try {
            File inputFile = new File(dirDoc);
            Scanner sc = new Scanner(inputFile);

            while(sc.hasNextLine()){
                String linea = sc.nextLine();
                if(!linea.trim().isEmpty()){
                    String[] partes = linea.split("#");
                    if(partes.length == 2){
                        ejecutarTarea(partes[0], partes[1]);

                        Thread.sleep(4000); // DORMIMOS 4 SEGUNDOS
                    }
                }else{
                    
                }
            }
        }catch(Exception e){
            System.out.println("Error durante la ejecución del fichero");
        }
    }



    public static void ejecutarTarea(String idWS, String duracion){
        
    }



    public static void ejecutarInteractivo(){
        Scanner consola = new Scanner(System.in);
        System.out.println("Modo interactivo. Por favor escriba 'idWS#duracion' o EXIT");


        while(true){
            System.out.println("=>");
            String linea = consola.nextLine();
            if(linea.toUpperCase().equals("EXIT") || linea.toUpperCase().equals("SALIR")){
                System.out.println("Saliendo...");
                break;
            }

            if(!linea.trim().isEmpty()){
                String[] partes = linea.split("#");
                if(partes.length == 2){
                    ejecutarTarea(partes[0], partes[1]);
                    try{
                        Thread.sleep(4000);
                    }catch(InterruptedException e){
                        System.out.println("ERROR EN LA ESPERA");
                    }
                }else{
                    System.out.println("ERROR. Formato inválido");
                }
            }
        }
        consola.close();
    }



}