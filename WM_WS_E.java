import java.net.*;
import java.io.*;
import java.util.Scanner;

public class WM_WS_E {
    private static volatile boolean averiaSimulada = false;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_E <IP_Kafka> <Puerto_Kafka> <IP_Monitor> <Puerto_Monitor>");
            System.exit(1);
        }

        // Ignoramos Kafka por ahora en esta fase de sockets
        String ipMonitor = args[2];
        int puertoMonitor = Integer.parseInt(args[3]);

        System.out.println("Iniciando Engine...");

        // Hilo independiente para leer el teclado y simular la avería de forma asíncrona
        Thread hiloTeclado = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Pulsa ENTER en cualquier momento para simular un KO (Fuga)...");
            scanner.nextLine();
            averiaSimulada = true;
            System.out.println("¡Avería simulada activada! El próximo reporte será KO.");
            scanner.close();
        });
        hiloTeclado.start();

        // Bucle principal: Conexión con el Monitor
        try {
            Socket skMonitor = new Socket(ipMonitor, puertoMonitor);
            DataInputStream flujoEntrada = new DataInputStream(skMonitor.getInputStream());
            DataOutputStream flujoSalida = new DataOutputStream(skMonitor.getOutputStream());
            System.out.println("Conectado al Monitor localmente.");

            while (true) {
                // Esperar el STATUS_CHECK del Monitor
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