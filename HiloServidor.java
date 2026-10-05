import java.net.Socket;
import java.io.*;


class HiloServidor extends Thread {
    private final Socket skCliente;
    private DataInputStream flujoEntrada;
    private DataOutputStream flujoSalida;
    private GestorCentral gestorCentral;

    public HiloServidor(Socket skCliente) {
        this.skCliente = skCliente;
    }

    @Override
    public void run() {
        try {
            flujoEntrada = new DataInputStream(skCliente.getInputStream());
            flujoSalida = new DataOutputStream(skCliente.getOutputStream());
            gestorCentral = GestorCentral.getInstance();

            String mensajeRecibido = leerSocket();
            //String respuesta = gestorCentral.procesarPeticion(mensajeRecibido);
            //escribirSocket(respuesta);

            while (mensajeRecibido != null && !mensajeRecibido.equals("exit")) {
                String respuesta = gestorCentral.procesarPeticion(mensajeRecibido);
                escribirSocket(respuesta);
                mensajeRecibido = leerSocket();

            }

        } catch (IOException e) {
            System.out.println("Error en el hilo del servidor: " + e.getMessage());
        } finally {
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


