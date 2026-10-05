import java.net.*;
import java.io.*;

public class WM_WS_M {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_M <Puerto_Escucha_Engine> <IP_Central> <Puerto_Central> <ID_WS>");
            System.exit(1);
        }

        int puertoEngine = Integer.parseInt(args[0]);
        String ipCentral = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        String idWS = args[3];

        Socket skCentral = null;
        DataOutputStream salidaCentral = null;

        // 1. Intento de conexion Central y Autenticación
        try {
            skCentral = new Socket(ipCentral, puertoCentral);
            salidaCentral = new DataOutputStream(skCentral.getOutputStream());
            System.out.println("Conexion con la central correcta");

            // Enviar petición de registro y alta
            String mensajeRegistro = "REGISTRO#" + idWS + "#Parque Central";
            String tramaRegistro = ProtocoloUtil.empaquetar(mensajeRegistro);
            salidaCentral.writeUTF(tramaRegistro);
            System.out.println("Petición de registro enviada a Central.");

        } catch (Exception e) {
            System.out.println("AVISO: LA CENTRAL NO ESTA CONECTADA, MODO AISLADO");
        }

        // 2. Levantar el servidor para escuchar el engine
        try {
            ServerSocket skServidor = new ServerSocket(puertoEngine);
            System.out.println("Monitor escuchando al Engine en el puerto " + puertoEngine);

            // espera hasta que conecte el engine (manda accept)
            Socket skEngine = skServidor.accept();
            System.out.println("Engine conectado al Monitor localmente.");

            iniciarBucleDeSalud(skEngine, salidaCentral, idWS);

        } catch(Exception e) {
            System.out.println("Error en el servidor del monitor: " + e.toString());
        }
    } 

    private static void iniciarBucleDeSalud(Socket skEngine, DataOutputStream salidaCentral, String idWS) {
        try {
            DataOutputStream flujoSalida = new DataOutputStream(skEngine.getOutputStream());
            DataInputStream flujoEntrada = new DataInputStream(skEngine.getInputStream());

            while (true) {
                // dormir 1 sec
                Thread.sleep(1000);

                String tramaEnvio = ProtocoloUtil.empaquetar("STATUS_CHECK");
                flujoSalida.writeUTF(tramaEnvio);
                
                String respuestaTrama = flujoEntrada.readUTF();
                String respuesta = ProtocoloUtil.desempaquetar(respuestaTrama);
                System.out.println("Engine responde: " + respuesta);

                // Si se detecta un KO mediante simulación por teclado en el Engine
                if (respuesta.equals("KO")) {
                    reportarFuga(salidaCentral, idWS);
                }
            }
        } catch (Exception e) {
            System.out.println("Conexion con engine perdida.");
            reportarFuga(salidaCentral, idWS);
        }
    }

    private static void reportarFuga(DataOutputStream salidaCentral, String idWS) {
        if (salidaCentral != null) {
            try {
                String mensajeFuga = "FUGA#" + idWS;
                String tramaFuga = ProtocoloUtil.empaquetar(mensajeFuga);
                salidaCentral.writeUTF(tramaFuga);
                System.out.println("Avería (fuga) reportada a la Central de forma inmediata.");
            } catch (Exception ex) {
                System.out.println("Error al comunicar la avería a la Central.");
            }
        } else {
            System.out.println("Avería detectada, pero el Monitor funciona en modo aislado (Central desconectada).");
        }
    }
}