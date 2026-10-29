import java.net.*;
import java.io.*;

public class WM_WS_M {
    private static boolean enFuga = false;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_M <Puerto_Escucha_Engine> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]");
            System.exit(1);
        }

        int puertoEngine = Integer.parseInt(args[0]);
        String ipCentral = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        String idWS = args[3];
        String ubicacion = (args.length >= 5) ? args[4] : "Sector Municipal";

        Socket skCentral = null;
        DataOutputStream salidaCentral = null;
        DataInputStream entradaCentral = null;

        // 1. Intento de conexión Central y Autenticación con reintentos
        for (int intento = 1; intento <= 5; intento++) {
            try {
                skCentral = new Socket(ipCentral, puertoCentral);
                salidaCentral = new DataOutputStream(skCentral.getOutputStream());
                entradaCentral = new DataInputStream(skCentral.getInputStream());
                System.out.println("Conexión con la Central correcta.");

                // Enviar petición de registro y alta
                String mensajeRegistro = "REGISTRO#" + idWS + "#" + ubicacion;
                String tramaRegistro = ProtocoloUtil.empaquetar(mensajeRegistro);
                salidaCentral.writeUTF(tramaRegistro);
                String ackTrama = entradaCentral.readUTF();
                String ack = ProtocoloUtil.desempaquetar(ackTrama);
                System.out.println("Petición de registro enviada a Central [" + idWS + " - " + ubicacion + "] -> Respuesta: " + ack);
                break;
            } catch (Exception e) {
                if (intento < 5) {
                    try { Thread.sleep(1500); } catch (InterruptedException ignored) {}
                } else {
                    System.out.println("AVISO: LA CENTRAL NO ESTÁ CONECTADA TRAS 5 INTENTOS, MODO AISLADO");
                }
            }
        }

        // 2. Levantar el servidor para escuchar el Engine
        try (ServerSocket skServidor = new ServerSocket(puertoEngine)) {
            System.out.println("Monitor escuchando al Engine en el puerto " + puertoEngine);

            // Espera hasta que conecte el Engine
            Socket skEngine = skServidor.accept();
            System.out.println("Engine conectado al Monitor localmente.");

            iniciarBucleDeSalud(skEngine, salidaCentral, entradaCentral, idWS);

        } catch (Exception e) {
            System.out.println("Error en el servidor del Monitor: " + e.toString());
        }
    }

    private static void iniciarBucleDeSalud(Socket skEngine, DataOutputStream salidaCentral, DataInputStream entradaCentral, String idWS) {
        try {
            DataOutputStream flujoSalida = new DataOutputStream(skEngine.getOutputStream());
            DataInputStream flujoEntrada = new DataInputStream(skEngine.getInputStream());

            while (true) {
                // Comprobación cada 1 segundo
                Thread.sleep(1000);

                String tramaEnvio = ProtocoloUtil.empaquetar("STATUS_CHECK");
                flujoSalida.writeUTF(tramaEnvio);

                String respuestaTrama = flujoEntrada.readUTF();
                String respuesta = ProtocoloUtil.desempaquetar(respuestaTrama);
                System.out.println("Engine [" + idWS + "] responde: " + respuesta);

                if (respuesta.equals("KO")) {
                    if (!enFuga) {
                        enFuga = true;
                        reportarFuga(salidaCentral, entradaCentral, idWS);
                    }
                } else if (respuesta.equals("OK")) {
                    // Si estaba en estado de fuga y la contingencia se ha resuelto, notificar a Central
                    if (enFuga) {
                        enFuga = false;
                        reportarRecuperacion(salidaCentral, entradaCentral, idWS);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Conexión con Engine perdida. Reportando incidencia a Central...");
            if (!enFuga) {
                enFuga = true;
                reportarFuga(salidaCentral, entradaCentral, idWS);
            }
        }
    }

    private static void reportarFuga(DataOutputStream salidaCentral, DataInputStream entradaCentral, String idWS) {
        if (salidaCentral != null) {
            try {
                String mensajeFuga = "FUGA#" + idWS;
                String tramaFuga = ProtocoloUtil.empaquetar(mensajeFuga);
                salidaCentral.writeUTF(tramaFuga);
                if (entradaCentral != null) {
                    String resp = ProtocoloUtil.desempaquetar(entradaCentral.readUTF());
                    System.out.println("Avería (fuga) reportada a la Central de forma inmediata -> Respuesta: " + resp);
                } else {
                    System.out.println("Avería (fuga) reportada a la Central de forma inmediata.");
                }
            } catch (Exception ex) {
                System.out.println("Error al comunicar la avería a la Central: " + ex.getMessage());
            }
        } else {
            System.out.println("Avería detectada, pero el Monitor funciona en modo aislado (Central desconectada).");
        }
    }

    private static void reportarRecuperacion(DataOutputStream salidaCentral, DataInputStream entradaCentral, String idWS) {
        if (salidaCentral != null) {
            try {
                String mensajeRecuperacion = "FIN_FUGA#" + idWS;
                String tramaRecuperacion = ProtocoloUtil.empaquetar(mensajeRecuperacion);
                salidaCentral.writeUTF(tramaRecuperacion);
                if (entradaCentral != null) {
                    String resp = ProtocoloUtil.desempaquetar(entradaCentral.readUTF());
                    System.out.println("Contingencia resuelta notificada a Central [" + idWS + "] -> Respuesta: " + resp);
                } else {
                    System.out.println("Contingencia resuelta notificada a Central [" + idWS + "].");
                }
            } catch (Exception ex) {
                System.out.println("Error al comunicar la resolución de avería a la Central: " + ex.getMessage());
            }
        }
    }
}