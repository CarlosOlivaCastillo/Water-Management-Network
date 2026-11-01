import java.net.*;
import java.io.*;

public class WM_WS_M {
    private static volatile boolean enFuga = false;
    private static volatile boolean centralConectada = false;
    private static Socket skCentral = null;
    private static DataOutputStream salidaCentral = null;
    private static DataInputStream entradaCentral = null;
    private static final Object lockCentral = new Object();

    private static String ipCentral;
    private static int puertoCentral;
    private static String idWS;
    private static String ubicacion;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_M <Puerto_Escucha_Engine> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]");
            System.exit(1);
        }

        int puertoEngine = Integer.parseInt(args[0]);
        ipCentral = args[1];
        puertoCentral = Integer.parseInt(args[2]);
        idWS = args[3];
        ubicacion = (args.length >= 5) ? args[4] : "Sector Municipal";

        // 1. Conexión inicial con WM_Central
        conectarConCentral();

        // 2. Hilo de vigilancia y reconexión automática con WM_Central
        Thread hiloCentral = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(2500);
                    if (!centralConectada) {
                        conectarConCentral();
                    } else {
                        synchronized (lockCentral) {
                            if (salidaCentral != null && entradaCentral != null) {
                                salidaCentral.writeUTF(ProtocoloUtil.empaquetar("PING#" + idWS));
                                String resp = ProtocoloUtil.desempaquetar(entradaCentral.readUTF());
                                if (resp == null) {
                                    throw new IOException("Sin respuesta de Central");
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    if (centralConectada) {
                        centralConectada = false;
                        System.out.println("[ERROR] Imposible conectar con WM_Central (conexión perdida). Reintentando reconexión...");
                    }
                    cerrarSocketCentral();
                }
            }
        }, "Hilo-ReconexionCentral");
        hiloCentral.setDaemon(true);
        hiloCentral.start();

        // 3. Levantar el servidor para escuchar al Engine de forma continua
        try (ServerSocket skServidor = new ServerSocket(puertoEngine)) {
            System.out.println("Monitor [" + idWS + "] escuchando al Engine en el puerto " + puertoEngine);

            while (true) {
                System.out.println("Esperando conexión del Engine [" + idWS + "] en el puerto " + puertoEngine + "...");
                Socket skEngine = skServidor.accept();
                System.out.println("Engine [" + idWS + "] conectado al Monitor.");

                iniciarBucleDeSalud(skEngine);

                try { skEngine.close(); } catch (Exception ignored) {}
                System.out.println("Monitor [" + idWS + "] sigue activo a la espera de que el Engine vuelva a conectarse...");
            }
        } catch (Exception e) {
            System.out.println("Error en el servidor del Monitor: " + e.toString());
        }
    }

    private static void conectarConCentral() {
        synchronized (lockCentral) {
            try {
                cerrarSocketCentral();
                skCentral = new Socket();
                skCentral.connect(new InetSocketAddress(ipCentral, puertoCentral), 2000);
                skCentral.setSoTimeout(3000);
                salidaCentral = new DataOutputStream(skCentral.getOutputStream());
                entradaCentral = new DataInputStream(skCentral.getInputStream());

                String mensajeRegistro = "REGISTRO#" + idWS + "#" + ubicacion;
                salidaCentral.writeUTF(ProtocoloUtil.empaquetar(mensajeRegistro));
                String ackTrama = entradaCentral.readUTF();
                String ack = ProtocoloUtil.desempaquetar(ackTrama);

                if ("<NACK>".equals(ack.trim())) {
                    System.out.println("[ERROR] Autenticación DENEGADA por WM_Central (<NACK>): La estación [" + idWS + "] no está registrada en la base de datos.");
                    centralConectada = false;
                    cerrarSocketCentral();
                    return;
                }

                centralConectada = true;
                System.out.println("Conexión y autenticación con WM_Central correcta [" + idWS + " - " + ubicacion + "] -> Respuesta: " + ack);

                // Si la estación estaba en fuga cuando cayó Central, re-informar del estado de fuga
                if (enFuga) {
                    salidaCentral.writeUTF(ProtocoloUtil.empaquetar("FUGA#" + idWS));
                    ProtocoloUtil.desempaquetar(entradaCentral.readUTF());
                }
            } catch (Exception e) {
                centralConectada = false;
                cerrarSocketCentral();
                System.out.println("[ERROR] Imposible conectar con WM_Central (" + ipCentral + ":" + puertoCentral + "). Modo autónomo activo.");
            }
        }
    }

    private static void cerrarSocketCentral() {
        try { if (entradaCentral != null) entradaCentral.close(); } catch (Exception ignored) {}
        try { if (salidaCentral != null) salidaCentral.close(); } catch (Exception ignored) {}
        try { if (skCentral != null && !skCentral.isClosed()) skCentral.close(); } catch (Exception ignored) {}
        entradaCentral = null;
        salidaCentral = null;
        skCentral = null;
    }

    private static void iniciarBucleDeSalud(Socket skEngine) {
        try {
            DataOutputStream flujoSalida = new DataOutputStream(skEngine.getOutputStream());
            DataInputStream flujoEntrada = new DataInputStream(skEngine.getInputStream());

            while (true) {
                Thread.sleep(1000);

                String tramaEnvio = ProtocoloUtil.empaquetar("STATUS_CHECK");
                flujoSalida.writeUTF(tramaEnvio);

                String respuestaTrama = flujoEntrada.readUTF();
                String respuesta = ProtocoloUtil.desempaquetar(respuestaTrama);
                System.out.println("Engine [" + idWS + "] responde: " + respuesta);

                if (respuesta.equals("KO")) {
                    if (!enFuga) {
                        enFuga = true;
                        reportarFuga();
                    }
                } else if (respuesta.equals("OK")) {
                    if (enFuga) {
                        enFuga = false;
                        reportarRecuperacion();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Conexión con Engine [" + idWS + "] perdida. Reportando avería (fuga) a WM_Central...");
            if (!enFuga) {
                enFuga = true;
                reportarFuga();
            }
        }
    }

    private static void reportarFuga() {
        synchronized (lockCentral) {
            if (centralConectada && salidaCentral != null) {
                try {
                    salidaCentral.writeUTF(ProtocoloUtil.empaquetar("FUGA#" + idWS));
                    if (entradaCentral != null) {
                        String resp = ProtocoloUtil.desempaquetar(entradaCentral.readUTF());
                        System.out.println("Avería (fuga) reportada a WM_Central de forma inmediata -> Respuesta: " + resp);
                    }
                } catch (Exception ex) {
                    centralConectada = false;
                    System.out.println("[ERROR] Imposible conectar con WM_Central al reportar fuga: " + ex.getMessage());
                }
            } else {
                System.out.println("[ERROR] Imposible conectar con WM_Central: Avería detectada localmente en [" + idWS + "]. Se notificará al reconectar.");
            }
        }
    }

    private static void reportarRecuperacion() {
        synchronized (lockCentral) {
            if (centralConectada && salidaCentral != null) {
                try {
                    salidaCentral.writeUTF(ProtocoloUtil.empaquetar("FIN_FUGA#" + idWS));
                    if (entradaCentral != null) {
                        String resp = ProtocoloUtil.desempaquetar(entradaCentral.readUTF());
                        System.out.println("Contingencia resuelta notificada a WM_Central [" + idWS + "] -> Respuesta: " + resp);
                    }
                } catch (Exception ex) {
                    centralConectada = false;
                    System.out.println("[ERROR] Imposible conectar con WM_Central al reportar recuperación: " + ex.getMessage());
                }
            } else {
                System.out.println("[ERROR] Imposible conectar con WM_Central: Recuperación registrada localmente en [" + idWS + "].");
            }
        }
    }
}