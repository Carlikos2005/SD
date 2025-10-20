import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseManager {
    private static final String DB_URL = "jdbc:sqlite:evcharging.db";
    
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL);
    }
    
    public static void initializeDatabase() {
        String createChargingPointsTable = """
            CREATE TABLE IF NOT EXISTS charging_points (
                id VARCHAR(50) PRIMARY KEY,
                location VARCHAR(100) NOT NULL,
                status VARCHAR(20) DEFAULT 'DESCONECTADO',
                price_per_kwh DECIMAL(5,2) DEFAULT 0.25,
                current_consumption DECIMAL(5,2) DEFAULT 0,
                current_amount DECIMAL(8,2) DEFAULT 0,
                current_driver VARCHAR(50),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
            
        String createDriversTable = """
            CREATE TABLE IF NOT EXISTS drivers (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                vehicle_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
            
        String createChargingSessionsTable = """
            CREATE TABLE IF NOT EXISTS charging_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id VARCHAR(50) NOT NULL,
                cp_id VARCHAR(50) NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                total_consumption DECIMAL(8,2) DEFAULT 0,
                total_amount DECIMAL(8,2) DEFAULT 0,
                status VARCHAR(20) DEFAULT 'ACTIVE',
                FOREIGN KEY (driver_id) REFERENCES drivers(id),
                FOREIGN KEY (cp_id) REFERENCES charging_points(id)
            )
            """;

        String createSystemEventsTable = """
            CREATE TABLE IF NOT EXISTS system_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type VARCHAR(50) NOT NULL,
                source_id VARCHAR(50) NOT NULL,
                description TEXT,
                event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
        
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(createChargingPointsTable);
            stmt.execute(createDriversTable);
            stmt.execute(createChargingSessionsTable);
            stmt.execute(createSystemEventsTable);
            
            insertSampleData();
            
            System.out.println("Base de datos inicializada correctamente");
            
        } catch (SQLException e) {
            System.err.println("Error inicializando BD: " + e.getMessage());
        }
    }
    
    private static void insertSampleData() {
        String insertCP = """
            INSERT OR IGNORE INTO charging_points (id, location, status, price_per_kwh) 
            VALUES (?, ?, ?, ?)
            """;
            
        String insertDriver = """
            INSERT OR IGNORE INTO drivers (id, name, vehicle_type) 
            VALUES (?, ?, ?)
            """;
        
        try (Connection conn = getConnection();
             PreparedStatement cpStmt = conn.prepareStatement(insertCP);
             PreparedStatement driverStmt = conn.prepareStatement(insertDriver)) {
            
            // Puntos de recarga
            String[][] chargingPoints = {
                {"CP-001", "Calle Principal 123", "ACTIVADO", "0.25"},
                {"CP-002", "Avenida Central 456", "ACTIVADO", "0.28"},
                {"CP-003", "Plaza Mayor 789", "PARADO", "0.26"}
            };
            
            for (String[] cp : chargingPoints) {
                cpStmt.setString(1, cp[0]);
                cpStmt.setString(2, cp[1]);
                cpStmt.setString(3, cp[2]);
                cpStmt.setDouble(4, Double.parseDouble(cp[3]));
                cpStmt.executeUpdate();
            }
            
            // Conductores
            String[][] drivers = {
                {"DRIVER-001", "Juan Pérez", "Tesla Model 3"},
                {"DRIVER-002", "María García", "Nissan Leaf"},
                {"DRIVER-003", "Carlos López", "BMW i3"}
            };
            
            for (String[] driver : drivers) {
                driverStmt.setString(1, driver[0]);
                driverStmt.setString(2, driver[1]);
                driverStmt.setString(3, driver[2]);
                driverStmt.executeUpdate();
            }
            
        } catch (SQLException e) {
            System.err.println("Error insertando datos: " + e.getMessage());
        }
    }
    
    // Métodos útiles para usar en tus apps
    public static void updateChargingPointStatus(String cpId, String status) {
        String sql = "UPDATE charging_points SET status = ?, last_updated = CURRENT_TIMESTAMP WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, status);
            pstmt.setString(2, cpId);
            pstmt.executeUpdate();
            
        } catch (SQLException e) {
            System.err.println("Error actualizando CP: " + e.getMessage());
        }
    }
    
    public static void updateChargingPointConsumption(String cpId, double consumption, double amount, String driverId) {
        String sql = "UPDATE charging_points SET current_consumption = ?, current_amount = ?, current_driver = ?, last_updated = CURRENT_TIMESTAMP WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setDouble(1, consumption);
            pstmt.setDouble(2, amount);
            pstmt.setString(3, driverId);
            pstmt.setString(4, cpId);
            pstmt.executeUpdate();
            
        } catch (SQLException e) {
            System.err.println("Error actualizando consumo: " + e.getMessage());
        }
    }
    
    public static List<String> getAllChargingPoints() {
        List<String> points = new ArrayList<>();
        String sql = "SELECT id, location, status FROM charging_points";
        
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                String point = String.format("CP: %s | Ubicación: %s | Estado: %s",
                    rs.getString("id"),
                    rs.getString("location"),
                    rs.getString("status"));
                points.add(point);
            }
            
        } catch (SQLException e) {
            System.err.println("Error obteniendo CPs: " + e.getMessage());
        }
        
        return points;
    }

    public static void createChargingSession(String driverId, String cpId) {
        String sql = "INSERT INTO charging_sessions (driver_id, cp_id, status) VALUES (?, ?, 'ACTIVE')";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, driverId);
            pstmt.setString(2, cpId);
            pstmt.executeUpdate();
            
        } catch (SQLException e) {
            System.err.println("Error creando sesión: " + e.getMessage());
        }
    }

    public static void finishChargingSession(String driverId, String cpId, double totalConsumption, double totalAmount) {
        String sql = "UPDATE charging_sessions SET end_time = CURRENT_TIMESTAMP, total_consumption = ?, total_amount = ?, status = 'FINISHED' WHERE driver_id = ? AND cp_id = ? AND status = 'ACTIVE'";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setDouble(1, totalConsumption);
            pstmt.setDouble(2, totalAmount);
            pstmt.setString(3, driverId);
            pstmt.setString(4, cpId);
            pstmt.executeUpdate();
            
        } catch (SQLException e) {
            System.err.println("Error finalizando sesión: " + e.getMessage());
        }
    }

    public static void logSystemEvent(String eventType, String sourceId, String description) {
        String sql = "INSERT INTO system_events (event_type, source_id, description) VALUES (?, ?, ?)";
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, eventType);
            pstmt.setString(2, sourceId);
            pstmt.setString(3, description);
            pstmt.executeUpdate();
            
        } catch (SQLException e) {
            System.err.println("Error registrando evento: " + e.getMessage());
        }
    }
}