import java.sql.*;
import java.util.*;


public final class GestorCentral{
    private static GestorCentral instancia;
    private Connection conexionBBDD;
    

    private GestorCentral() {
        try {
            // Cargar el driver JDBC
            Class.forName("org.sqlite.JDBC");
            // Establecer la conexión a la base de datos
            conexionBBDD = DriverManager.getConnection("jdbc:sqlite:watermanagement.db");
            crearTablasIniciales();
            cargarDatos();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Error al conectar con la base de datos: " + e.getMessage());
        }
    }


    public void crearTablasIniciales() {
    String sql = "CREATE TABLE IF NOT EXISTS estaciones ("
                + "idWS TEXT PRIMARY KEY, "
                + "ubicacion TEXT, "
                + "estado TEXT, "
                + "idFO TEXT" // PARA GUARDAR QUIEN ACTIVA EL RIEGO
                + ");";
    try (PreparedStatement pstmt = conexionBBDD.prepareStatement(sql)) {
        
        pstmt.execute(); 
        // System.out.println("Base de datos y tablas inicializadas correctamente.");
        
    } catch (SQLException e) {
        // System.out.println("Error fatal al crear las tablas: " + e.getMessage());
    }
}

    public static synchronized  GestorCentral getInstance() {
        if (instancia == null) {
            instancia = new GestorCentral();
        }
        return instancia;
    }


    public synchronized boolean cargarDatos(){
        String com = "UPDATE estaciones SET estado = 'DISPONIBLE', idFO = NULL";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("Error al cargar los datos: " + e.getMessage());
            return false;
        }
    }


    public synchronized String procesarPeticion(String mensaje){
        boolean esvalido = false;
        String [] partes = mensaje.split("#");
        if (partes.length == 0) {
            return "<NACK>";
        }
        String peticion = partes[0];

        switch(peticion) {
            case "REGISTRO":
                // REGISTRO#IDWS#UBICACION
                if(partes.length == 3){
                    String idWS = partes[1];
                    String ubicacion = partes[2];
                    esvalido = añadirDatos(idWS, "DISPONIBLE", ubicacion);
                    if(esvalido){
                        return "<ACK>";
                    }else{
                        return "<NACK>";
                    }
                }
                break;
            case "START_RIEGO":
                // START_RIEGO#IDWS#IDFO#DURACION
                if(partes.length == 4){
                    String idWS = partes[1];
                    String idFO = partes[2];
                    int duracion = Integer.parseInt(partes[3]);
                    esvalido = autorizarRiego(idWS, idFO, duracion);
                }
                break;
            case "AVERIA":
                // AVERIA#IDWS
                if(partes.length == 2){
                    String idWS = partes[1];
                    esvalido = actualizarEstado(idWS, "AVERIADO");
                }
                break;
            case "STOP_RIEGO":
                // STOP_RIEGO#IDWS
                if(partes.length == 2){
                    String idWS = partes[1];
                    esvalido = actualizarEstado(idWS, "DISPONIBLE");
                }
                break;
            default:
                return "<NACK>";
        }

        if(esvalido){
            return "<ACK>";
        }else{
            return "<NACK>";
        }
    }


    public synchronized boolean añadirDatos(String idWS, String estado, String ubicacion){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized boolean actualizarEstado(String idWS, String estado){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized boolean estaDisponible(String idWS){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized boolean autorizarRiego(String idWS, String idFO, int duracion){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized List<String> obtenerEstaciones(){
        throw new UnsupportedOperationException("Not implemented yet");
    }

}