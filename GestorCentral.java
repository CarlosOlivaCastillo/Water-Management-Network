import java.sql.*;
import java.util.*;


public class GestorCentral{
    private static GestorCentral instancia;
    private Connection conexionBBDD;
    

    private GestorCentral() {
        try {
            // Cargar el driver JDBC
            Class.forName("org.sqlite.JDBC");
            // Establecer la conexión a la base de datos
            conexionBBDD = DriverManager.getConnection("jdbc:sqlite:watermanagement.db");
        } catch (ClassNotFoundException | SQLException e) {
        }
    }

    public static synchronized  GestorCentral getInstance() {
        if (instancia == null) {
            instancia = new GestorCentral();
        }
        return instancia;
    }


    public synchronized boolean cargarDatos(){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized String procesarPeticion(String mensaje){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized void añadirDatos(String idWS, String estado, String ubicacion){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized void actualizarEstado(String idWS, String estado){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized boolean estaDisponible(String idWS){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized boolean autorizarRiego(String idWS, String idFO){
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public synchronized List<String> obtenerEstaciones(){
        throw new UnsupportedOperationException("Not implemented yet");
    }

}