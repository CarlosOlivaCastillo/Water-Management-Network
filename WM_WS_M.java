import java.net.*;
import java.io.*;

public class WM_WS_M{
    public static void main(String[] args){
        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_M <Puerto_Escucha_Engine> <IP_Central> <Puerto_Central> <ID_WS>");
            System.exit(1);
        }

        int puertoEngine = Integer.parseInt(args[0]);
        String ipCentral = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        String idWS = args[3];

        //1     Intento de conexion Central
        try {
            Socket skCentral = new Socket(ipCentral, puertoCentral);
            System.out.println("Conexion con la central correcta");

        } catch (Exception e){
            System.out.println("AVISO LA CENTRAL NO ESTA CONECTADA, MODO AISLADO");
        }

        //2     Levantar el servidor para escuchar el engine
        try {
            ServerSocket skServidor = new ServerSocket(puertoEngine);
            System.out.println("Monitor escuchando al Engine en el puerto " + puertoEngine);

            // espera hasta que conecte el engine (manda accept)
            Socket skEngine = skServidor.accept();
            System.out.println("Engine conectado al Monitor localmente.");

            iniciarBucleDeSalud(skEngine);

        } catch(Exception e){
            System.out.println("Error en el servidor del monitor: " + e.toString());
        }
    } 

    private static void iniciarBucleDeSalud(Socket skEngine){
        try {
            DataOutputStream flujoSalida = new DataOutputStream(skEngine.getOutputStream());
            DataInputStream flujoEntrada = new DataInputStream(skEngine.getInputStream());

            while (true) {
                //dormir 1 sec
                Thread.sleep(1000);

                flujoSalida.writeUTF("<STX>STATUS_CHECK<ETX>LRC_AQUI");
                String respuesta = flujoEntrada.readUTF();
                System.out.println("Engine responde: " + respuesta);
            }
        } catch (Exception e){
            System.out.println("conexion con engine perdida");
        }
    }
}