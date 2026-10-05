public class ProtocoloUtil {
    public static final char STX = 0x02;
    public static final char ETX = 0x03;

    public static String empaquetar(String mensaje) {
        char lrc = calcularLRC(mensaje);
        return STX + mensaje + ETX + lrc;
    }

    public static String desempaquetar(String trama) throws Exception {
        if (trama == null || trama.length() < 3 || trama.charAt(0) != STX) {
            throw new Exception("Formato de trama no válido");
        }
        
        int finMensaje = trama.indexOf(ETX);
        if (finMensaje == -1) {
            throw new Exception("No se encontró el delimitador de fin de texto (ETX)");
        }
        
        String mensaje = trama.substring(1, finMensaje);
        char lrcRecibido = trama.charAt(finMensaje + 1);
        
        if (calcularLRC(mensaje) != lrcRecibido) {
            throw new Exception("Fallo de integridad: LRC incorrecto");
        }
        return mensaje;
    }

    private static char calcularLRC(String mensaje) {
        char lrc = 0;
        for (int i = 0; i < mensaje.length(); i++) {
            lrc ^= mensaje.charAt(i);
        }
        return lrc;
    }
}