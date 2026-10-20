import java.net.Socket;
import java.io.*;


class HiloServidor extends Thread {
    private final Socket skCliente;
    private DataInputStream flujoEntrada;
    private DataOutputStream flujoSalida;
    private GestorCentral gestorCentral;
    private String idWS = null;

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
                    String mensaje = ProtocoloUtil.desempaquetar(tramaRecibida);
                    if(mensaje.trim().isEmpty()){
                        // De esta forma nos aseguramos que si cae el servidor se cierra la conexión
                        String[] partes = mensaje.split("#");
                        idWS = partes[1].trim();
                    }
                    String respuesta = gestorCentral.procesarPeticion(mensaje);
                    escribirSocket(ProtocoloUtil.empaquetar(respuesta));
                } catch (Exception e) {
                    System.out.println("Trama corrupta o LRC inválido: " + e.getMessage());
                    escribirSocket("<NACK>");
                }
                tramaRecibida = leerSocket();
            }

        } catch (IOException e) {
            System.out.println("Error en el hilo del servidor: " + e.getMessage());
        } finally {
            gestorCentral.actualizarEstado(idWS, "DESCONECTADA");
            cerrarConexion();
        }
    }

    private String leerSocket() {
        String mensaje = null;
        try {
            mensaje = flujoEntrada.readUTF();
        } catch (IOException e) {
            System.out.println("Error al leer del socket: " + e.getMessage());
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


