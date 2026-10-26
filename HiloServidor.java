import java.net.Socket;
import java.io.*;

class HiloServidor extends Thread {
    private final Socket skCliente;
    private DataInputStream flujoEntrada;
    private DataOutputStream flujoSalida;
    private GestorCentral gestorCentral;
    private String idWS = null;
    private boolean esMonitor = false;

    public HiloServidor(Socket skCliente) {
        this.skCliente = skCliente;
    }

    @Override
    public void run() {
        try {
            flujoEntrada = new DataInputStream(skCliente.getInputStream());
            flujoSalida = new DataOutputStream(skCliente.getOutputStream());
            gestorCentral = GestorCentral.getInstance();

            String tramaRecibida = leerSocket();

            while (tramaRecibida != null && !tramaRecibida.equals("exit")) {
                try {
                    String accion = null;
                    String mensaje = ProtocoloUtil.desempaquetar(tramaRecibida);
                    if (!mensaje.trim().isEmpty()) {
                        String[] partes = mensaje.split("#");
                        accion = partes[0].trim();
                        if (partes.length > 1) {
                            idWS = partes[1].trim();
                        }
                        if ("REGISTRO".equals(accion) || "FUGA".equals(accion) || "AVERIA".equals(accion)) {
                            esMonitor = true;
                        }
                    }
                    String respuesta = gestorCentral.procesarPeticion(mensaje);
                    String[] partes = respuesta.split("#");
                    if ("FUGA".equals(accion) || "AVERIA".equals(accion)) {
                        if (partes.length > 1 && partes[1].trim().equals("R")) {
                            WM_Central.cortarAguaFuga(idWS);
                        } else {
                            ServidorWebCentral.registrarEvento("¡ALERTA! Fuga reportada en estación " + idWS);
                        }
                    } else if ("REGISTRO".equals(accion) && idWS != null) {
                        ServidorWebCentral.registrarEvento("Estación registrada y conectada: " + idWS);
                    }
                    escribirSocket(ProtocoloUtil.empaquetar(partes[0].trim()));
                } catch (Exception e) {
                    System.out.println("Trama corrupta o LRC inválido: " + e.getMessage());
                    escribirSocket(ProtocoloUtil.empaquetar("<NACK>"));
                }
                tramaRecibida = leerSocket();
            }

        } catch ( Exception e) {
            System.out.println("Error en el hilo del servidor: " + e.getMessage());
        } finally {
            if (esMonitor && idWS != null) {
                gestorCentral.actualizarEstado(idWS, "DESCONECTADA");
                ServidorWebCentral.registrarEvento("Estación desconectada: " + idWS);
            }
            cerrarConexion();
        }
    }

    private String leerSocket() {
        String mensaje = null;
        try {
            mensaje = flujoEntrada.readUTF();
        } catch (IOException e) {
            // Fin de stream o desconexión del cliente
        }
        return mensaje;
    }

    private void escribirSocket(String mensaje) {
        try {
            flujoSalida.writeUTF(mensaje);
        } catch (IOException e) {
            System.out.println("Error al escribir en el socket: " + e.getMessage());
        }
    }

    private void cerrarConexion() {
        try {
            if (flujoEntrada != null) flujoEntrada.close();
            if (flujoSalida != null) flujoSalida.close();
            if (skCliente != null) skCliente.close();
        } catch (IOException e) {
            System.out.println("Error al cerrar la conexión: " + e.getMessage());
        }
    }
}
