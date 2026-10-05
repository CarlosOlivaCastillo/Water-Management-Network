import java.io.*;


class WM_FO{
    private String ipKafka;
    private String puertoKafka;
    private String idFO;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Debe indicar la IP de Kafka, el puerto de Kafka y el ID del FO");
            System.out.println("$./WM_FO <ip_kafka> <puerto_kafka> <idFO> <idWS> <duracion> \n");
            System.out.println("$./WM_FO <ip_kafka> <puerto_kafka> <idFO> [documento] \n");
            System.exit (1);
        }
        String ipKafka = args[0];
        String puertoKafka = args[1];
        String idFO = args[2];
        String idWS, duracion;

        if(args.length == 4){
            String dirDoc = args[3];
            // Ejecutar tarea

        }else if(args.length == 5){
            idWS = args[3];
            duracion = args[4];
            // Ejecutar tarea
        }

    }

}