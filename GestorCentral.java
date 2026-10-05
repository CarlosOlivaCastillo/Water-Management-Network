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
        String com = "UPDATE estaciones SET estado = 'DESCONECTADA', idFO = NULL";
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
            case "REGISTRO" -> {
                // REGISTRO#IDWS#UBICACION
                if(partes.length == 3){
                    String idWS = partes[1];
                    String ubicacion = partes[2];
                    esvalido = añadirDatos(idWS, "DISPONIBLE", ubicacion);
                }
            }
            case "START_RIEGO" -> {
                // START_RIEGO#IDWS#IDFO#DURACION
                if(partes.length == 4){
                    String idWS = partes[1];
                    String idFO = partes[2];
                    int duracion;
                    try{
                        duracion = Integer.parseInt(partes[3]);
                    }catch(NumberFormatException e){
                        esvalido = false;
                        break;
                    }
                    esvalido = autorizarRiego(idWS, idFO, duracion);
                }
            }
            case "AVERIA" -> {
                // AVERIA#IDWS
                if(partes.length == 2){
                    String idWS = partes[1];
                    esvalido = actualizarEstado(idWS, "FUGA");
                }
            }
            case "STOP_RIEGO" -> {
                // STOP_RIEGO#IDWS
                if(partes.length == 2){
                    String idWS = partes[1];
                    esvalido = actualizarEstado(idWS, "DISPONIBLE");
                }
            }
            default -> {
                esvalido = false;
            }
        }

        if(esvalido){
            return "<ACK>";
        }else{
            return "<NACK>";
        }
    }


    public synchronized boolean añadirDatos(String idWS, String estado, String ubicacion){
        // INSERT OR REPLACE para que en caso de existir no de error
        String com = "INSERT OR REPLACE INTO estaciones (idWS, estado, ubicacion) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, idWS);
            pstmt.setString(2, estado);
            pstmt.setString(3, ubicacion);
            int filasAfectadas = pstmt.executeUpdate();
            return filasAfectadas > 0;
        } catch (SQLException e) {
            System.out.println("Error al cargar los datos: " + e.getMessage());
            return false;
        }    
    }

    public synchronized boolean actualizarEstado(String idWS, String estado){
        String com = "UPDATE estaciones SET estado = ? WHERE idWS = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, estado);
            pstmt.setString(2, idWS);
            int filasAfectadas = pstmt.executeUpdate();
            return filasAfectadas > 0;
        } catch (SQLException e) {
            System.out.println("Error al actualizar el estado: " + e.getMessage());
            return false;
        }
    }

    public synchronized boolean estaDisponible(String idWS){
        String com = "SELECT estado FROM estaciones WHERE idWS = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, idWS);
            try (ResultSet rs = pstmt.executeQuery()) { // TABLA CON LOS RESULTADOS DE LA CONSULTA
                if (rs.next()) { // CONSULTAMOS LA PRIMERA
                    String estado = rs.getString("estado");
                    return "DISPONIBLE".equals(estado);
                }
            }
        } catch (SQLException e) {
            System.out.println("Error al verificar disponibilidad: " + e.getMessage());
        }
        return false;
    }

    public synchronized boolean autorizarRiego(String idWS, String idFO, int duracion){
        if(estaDisponible(idWS)){
            String com = "UPDATE estaciones SET estado = 'REGANDO', idFO = ? WHERE idWS = ?";
            try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
                pstmt.setString(1, idFO);
                pstmt.setString(2, idWS);
                int filasAfectadas = pstmt.executeUpdate();
                return filasAfectadas > 0;
            } catch (SQLException e) {
                System.out.println("Error al autorizar riego: " + e.getMessage());
            }
        }
        return false;
    }

    public synchronized List<String> obtenerEstaciones(){
        List<String> estaciones = new ArrayList<>();
        String com = "SELECT idWS, estado, ubicacion FROM estaciones";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)){
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String idWS = rs.getString("idWS");
                    String estado = rs.getString("estado");
                    String ubicacion = rs.getString("ubicacion");
                    estaciones.add(idWS + "#" + estado + "#" + ubicacion);
                }
            } catch (SQLException e) {
                System.out.println("Error al obtener estaciones: " + e.getMessage());
            }
        }catch (SQLException e) {
            System.out.println("Error al preparar la consulta: " + e.getMessage());
        }
        return estaciones;
    }

}