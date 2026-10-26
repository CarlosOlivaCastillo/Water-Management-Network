import java.sql.*;
import java.util.*;

public final class GestorCentral {
    private static GestorCentral instancia;
    private Connection conexionBBDD;

    private GestorCentral() {
        try {
            // Cargar el driver JDBC
            Class.forName("org.sqlite.JDBC");
            // Establecer la conexión a la base de datos
            conexionBBDD = DriverManager.getConnection("jdbc:sqlite:watermanagement.db");
            crearTablasIniciales();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("ERROR al conectar con la base de datos: " + e.getMessage());
        }
    }

    public void crearTablasIniciales() {
        String sqlEstaciones = "CREATE TABLE IF NOT EXISTS estaciones ("
                + "idWS TEXT PRIMARY KEY, "
                + "ubicacion TEXT, "
                + "estado TEXT, "
                + "idFO TEXT, " // PARA GUARDAR QUIEN ACTIVA EL RIEGO
                + "duracion INTEGER DEFAULT 0, "
                + "fecha_inicio TEXT"
                + ");";
        String sqlOperarios = "CREATE TABLE IF NOT EXISTS operarios ("
                + "idFO TEXT PRIMARY KEY, "
                + "nombre TEXT"
                + ");";
        try (Statement pstmt = conexionBBDD.createStatement()) {
            pstmt.execute(sqlEstaciones);
            pstmt.execute(sqlOperarios);
            // Asegurar columnas nuevas si la BD ya existía de versiones previas
            try { pstmt.execute("ALTER TABLE estaciones ADD COLUMN duracion INTEGER DEFAULT 0"); } catch (SQLException ignored) {}
            try { pstmt.execute("ALTER TABLE estaciones ADD COLUMN fecha_inicio TEXT"); } catch (SQLException ignored) {}

            pstmt.execute("INSERT OR IGNORE INTO operarios (idFO, nombre) VALUES ('FO_01', 'Operario Norte')");
            pstmt.execute("INSERT OR IGNORE INTO operarios (idFO, nombre) VALUES ('FO_02', 'Operario Sur')");
            pstmt.execute("INSERT OR IGNORE INTO operarios (idFO, nombre) VALUES ('FO_03', 'Operario Este')");
            pstmt.execute("INSERT OR IGNORE INTO operarios (idFO, nombre) VALUES ('WEB_PANEL', 'Panel Web Central')");
            pstmt.execute("INSERT OR IGNORE INTO operarios (idFO, nombre) VALUES ('CENTRAL', 'Consola Central')");
        } catch (SQLException e) {
            System.out.println("ERROR al crear las tablas: " + e.getMessage());
        }
    }

    public static synchronized GestorCentral getInstance() {
        if (instancia == null) {
            instancia = new GestorCentral();
        }
        return instancia;
    }

    public synchronized boolean cargarDatos() {
        try {
            // 1. Eliminar estaciones de prueba antiguas
            String sqlClean = "DELETE FROM estaciones WHERE idWS LIKE '%TEST%' OR idWS LIKE '%E2E%'";
            try (PreparedStatement pstmt = conexionBBDD.prepareStatement(sqlClean)) {
                pstmt.executeUpdate();
            }

            // 2. Marcar estaciones como DESCONECTADA al reiniciar Central
            String com = "UPDATE estaciones SET estado = 'DESCONECTADA', idFO = NULL, duracion = 0, fecha_inicio = NULL";
            try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
                pstmt.executeUpdate();
            }

            // 3. Inicializar las 7 estaciones oficiales si no existen
            String[][] estacionesBase = {
                {"WS_01", "Parque Central - Norte"},
                {"WS_02", "Parque Central - Sur"},
                {"WS_03", "Jardines del Prado"},
                {"WS_04", "Av. de las Palmeras"},
                {"WS_05", "Plaza Mayor"},
                {"WS_06", "Parque Fluvial Este"},
                {"WS_07", "Vivero Municipal"}
            };

            String sqlInsert = "INSERT OR IGNORE INTO estaciones (idWS, estado, ubicacion, idFO, duracion, fecha_inicio) VALUES (?, 'DESCONECTADA', ?, NULL, 0, NULL)";
            try (PreparedStatement pstmt = conexionBBDD.prepareStatement(sqlInsert)) {
                for (String[] est : estacionesBase) {
                    pstmt.setString(1, est[0]);
                    pstmt.setString(2, est[1]);
                    pstmt.executeUpdate();
                }
            }

            return true;
        } catch (SQLException e) {
            System.out.println("ERROR al cargar los datos: " + e.getMessage());
            return false;
        }
    }

    public synchronized boolean validarOperario(String idFO) {
        if (idFO == null || idFO.isEmpty()) {
            return false;
        }
        if (idFO.equalsIgnoreCase("CENTRAL") || idFO.equalsIgnoreCase("WEB_PANEL")) {
            return true;
        }
        String com = "SELECT idFO FROM operarios WHERE idFO = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, idFO);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            System.out.println("ERROR al validar operario: " + e.getMessage());
            return false;
        }
    }

    public synchronized String procesarPeticion(String mensaje) {
        boolean esvalido = false;
        if (mensaje == null || mensaje.trim().isEmpty()) {
            return "<NACK>";
        }
        String[] partes = mensaje.split("#");
        String peticion = partes[0].trim();

        switch (peticion) {
            case "REGISTRO" -> {
                // REGISTRO#IDWS#UBICACION
                if (partes.length == 3) {
                    String idWS = partes[1].trim();
                    String ubicacion = partes[2].trim();
                    esvalido = añadirDatos(idWS, "DISPONIBLE", ubicacion);
                }
            }
            case "START_RIEGO" -> {
                // START_RIEGO#IDWS#IDFO#DURACION
                if (partes.length == 4) {
                    String idWS = partes[1].trim();
                    String idFO = partes[2].trim();
                    int duracion;
                    try {
                        duracion = Integer.parseInt(partes[3].trim());
                    } catch (NumberFormatException e) {
                        return "<NACK>";
                    }
                    if (validarOperario(idFO)) {
                        esvalido = autorizarRiego(idWS, idFO, duracion);
                    }
                }
            }
            case "AVERIA", "FUGA" -> {
                // AVERIA#IDWS o FUGA#IDWS
                if (partes.length == 2) {
                    String idWS = partes[1].trim();
                    String actual = estaRegando(idWS) ? "R" : "D";
                    esvalido = actualizarEstado(idWS, "FUGA");
                    String res = esvalido ? "<ACK>" : "<NACK>";
                    return res + "#" + actual;
                }
            }
            case "STOP_RIEGO" -> {
                // STOP_RIEGO#IDWS[#idFO]
                if (partes.length >= 2) {
                    String idWS = partes[1].trim();
                    esvalido = finalizarRiego(idWS);
                }
            }
            case "DESCONEXION" -> {
                if (partes.length == 2) {
                    String idWS = partes[1].trim();
                    esvalido = actualizarEstado(idWS, "DESCONECTADA");
                }
            }
            case "BLOQUEAR" -> {
                if (partes.length == 2) {
                    String idWS = partes[1].trim();
                    String actual = estaRegando(idWS) ? "R" : "D";
                    esvalido = actualizarEstado(idWS, "FUERA_DE_SERVICIO");
                    String res = esvalido ? "<ACK>" : "<NACK>";
                    return res + "#" + actual;
                }
            }
            case "ACTIVAR" -> {
                if (partes.length == 2) {
                    String idWS = partes[1].trim();
                    esvalido = actualizarEstado(idWS, "DISPONIBLE");
                }
            }
            default -> {
                esvalido = false;
            }
        }

        if (esvalido) {
            return "<ACK>";
        } else {
            return "<NACK>";
        }
    }

    public synchronized boolean añadirDatos(String idWS, String estado, String ubicacion) {
        String com = "INSERT OR REPLACE INTO estaciones (idWS, estado, ubicacion, idFO, duracion, fecha_inicio) "
                + "VALUES (?, ?, ?, NULL, 0, NULL)";
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

    public synchronized boolean actualizarEstado(String idWS, String estado) {
        String com = "UPDATE estaciones SET estado = ?, "
                + "idFO = CASE WHEN ? IN ('DISPONIBLE', 'DESCONECTADA', 'FUERA_DE_SERVICIO', 'FUERA DE SERVICIO', 'FUGA') THEN NULL ELSE idFO END, "
                + "duracion = CASE WHEN ? IN ('DISPONIBLE', 'DESCONECTADA', 'FUERA_DE_SERVICIO', 'FUERA DE SERVICIO', 'FUGA') THEN 0 ELSE duracion END "
                + "WHERE idWS = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, estado);
            pstmt.setString(2, estado);
            pstmt.setString(3, estado);
            pstmt.setString(4, idWS);
            int filasAfectadas = pstmt.executeUpdate();
            return filasAfectadas > 0;
        } catch (SQLException e) {
            System.out.println("Error al actualizar el estado: " + e.getMessage());
            return false;
        }
    }

    public synchronized boolean estaDisponible(String idWS) {
        String com = "SELECT estado FROM estaciones WHERE idWS = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, idWS);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String estado = rs.getString("estado");
                    return estado != null && "DISPONIBLE".equals(estado.toUpperCase());
                }
            }
        } catch (SQLException e) {
            System.out.println("Error al verificar disponibilidad: " + e.getMessage());
        }
        return false;
    }

    public synchronized boolean estaRegando(String idWS) {
        String com = "SELECT estado FROM estaciones WHERE idWS = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, idWS);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String estado = rs.getString("estado");
                    return estado != null && "REGANDO".equals(estado.toUpperCase());
                }
            }
        } catch (SQLException e) {
            System.out.println("ERROR al verificar si estaba regando: " + e.getMessage());
        }
        return false;
    }

    public synchronized String obtenerOperario(String idWS) {
        String com = "SELECT idFO FROM estaciones WHERE idWS = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, idWS);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("idFO");
                }
            }
        } catch (SQLException e) {
            System.out.println("ERROR al obtener el operario: " + e.getMessage());
        }
        return null;
    }

    public synchronized boolean autorizarRiego(String idWS, String idFO, int duracion) {
        if (estaDisponible(idWS)) {
            String com = "UPDATE estaciones SET estado = 'REGANDO', idFO = ?, duracion = ?, "
                    + "fecha_inicio = datetime('now', 'localtime') WHERE idWS = ?";
            try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
                pstmt.setString(1, idFO);
                pstmt.setInt(2, duracion);
                pstmt.setString(3, idWS);
                int filasAfectadas = pstmt.executeUpdate();
                return filasAfectadas > 0;
            } catch (SQLException e) {
                System.out.println("ERROR al autorizar riego: " + e.getMessage());
            }
        }
        return false;
    }

    public synchronized boolean finalizarRiego(String idWS) {
        String com = "UPDATE estaciones SET estado = 'DISPONIBLE', idFO = NULL, duracion = 0, fecha_inicio = NULL WHERE idWS = ?";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            pstmt.setString(1, idWS);
            int filasAfectadas = pstmt.executeUpdate();
            return filasAfectadas > 0;
        } catch (SQLException e) {
            System.out.println("ERROR al finalizar el riego: " + e.getMessage());
            return false;
        }
    }

    public synchronized List<String> obtenerEstaciones() {
        List<String> estaciones = new ArrayList<>();
        String com = "SELECT idWS, estado, ubicacion, idFO FROM estaciones";
        try (PreparedStatement pstmt = conexionBBDD.prepareStatement(com)) {
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String idWS = rs.getString("idWS");
                    String estado = rs.getString("estado");
                    String ubicacion = rs.getString("ubicacion");
                    String idFO = rs.getString("idFO");
                    estaciones.add(idWS + "#" + estado + "#" + ubicacion + "#" + (idFO != null ? idFO : "-"));
                }
            } catch (SQLException e) {
                System.out.println("ERROR al obtener estaciones: " + e.getMessage());
            }
        } catch (SQLException e) {
            System.out.println("ERROR al preparar la consulta: " + e.getMessage());
        }
        return estaciones;
    }
}