// EV_Central.java
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EV_Central {

    private static final String CPS_FILE = "cps.json";
    private static final Map<String, CPState> cpStates = new HashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Uso: java EV_Central <puerto_escucha> <kafka_bootstrap>");
            System.exit(1);
        }

        int listenPort = Integer.parseInt(args[0]);
        String kafkaBootstrap = args[1];
        
        DatabaseManager.initializeDatabase();

        System.out.println("EV_Central iniciada");
        System.out.println("Puerto de escucha: " + listenPort);
        System.out.println("Kafka: " + kafkaBootstrap);
        loadCPsFromFile();

        // Hilo para el servidor de sockets
        ExecutorService executor = Executors.newCachedThreadPool();
        new Thread(() -> startSocketServer(listenPort, executor)).start();

        // Hilo para consumir Kafka
        new Thread(() -> consumeKafkaEvents(kafkaBootstrap)).start();

        // Menú de consola en hilo principal
        ScannerWrapper.startConsoleMenu();
    }

    private static void startSocketServer(int port, ExecutorService executor) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("EV_Central escuchando en puerto " + port + " (sockets)...");
            while (true) {
                Socket client = serverSocket.accept();
                executor.submit(new ClientHandler(client));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void consumeKafkaEvents(String kafkaBootstrap) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "evcentral-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("evcharging"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode msg = mapper.readTree(record.value());
                        String type = msg.get("type").asText();

                        switch (type) {
                            case "TELEMETRY":
                                handleTelemetry(msg);
                                break;
                            case "FAULT":
                                handleFault(msg);
                                break;
                            case "HEALTH_OK":
                                handleHealthOk(msg);
                                break;
                            case "CHARGING_END":
                                handleChargingEnd(msg);
                                break;
                            default:
                                System.out.println("Evento Kafka desconocido: " + type);
                        }
                        printPanel();
                    } catch (Exception e) {
                        System.err.println("Error procesando mensaje Kafka: " + record.value());
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    // --- Manejo de mensajes por socket ---
    static class ClientHandler implements Runnable {
        private final Socket socket;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String line = in.readLine();
                if (line == null) return;

                if (line.startsWith("REGISTER")) {
                    String[] parts = line.split("#");
                    if (parts.length >= 2) {
                        String cpId = parts[1];
                        cpStates.put(cpId, new CPState(cpId, "ACTIVADO"));
                        saveCPsToFile();
                        System.out.println("CP registrado por socket: " + cpId);
                        out.println("ACK");
                    } else {
                        out.println("NACK");
                    }
                } else if (line.startsWith("REQUEST")) {
                    String[] parts = line.split("#");
                    if (parts.length >= 3) {
                        String cpId = parts[1];
                        String driverId = parts[2];
                        CPState state = cpStates.get(cpId);
                        if (state != null && "ACTIVADO".equals(state.status)) {

                            DatabaseManager.updateChargingPointStatus(cpId, "SUMINISTRANDO");
                            DatabaseManager.updateChargingPointConsumption(cpId, 0.0, 0.0, driverId);
                            DatabaseManager.createChargingSession(driverId, cpId);
                            DatabaseManager.logSystemEvent("CHARGING_STARTED", cpId, "Conductor " + driverId + " inició carga");

                            System.out.println("Autorizando recarga para conductor " + driverId + " en " + cpId);
                            out.println("AUTHORIZED");
                        } else {
                            System.out.println("CP " + cpId + " no disponible");
                            out.println("DENIED");
                        }
                    } else {
                        out.println("NACK");
                    }
                }
                printPanel();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }

    // --- Manejo de eventos Kafka ---
    private static void handleTelemetry(JsonNode msg) {
        String cpId = msg.get("cpId").asText();
        CPState state = cpStates.get(cpId);
        if (state != null) {
            state.status = "SUMINISTRANDO";
            state.powerKw = msg.get("powerKw").asDouble();
            state.costEur = msg.get("costEur").asDouble();
            state.driverId = msg.has("driverId") ? msg.get("driverId").asText() : "";

            DatabaseManager.updateChargingPointConsumption(cpId, state.powerKw, state.costEur, state.driverId);
        }

    }

    private static void handleFault(JsonNode msg) {
        String cpId = msg.get("cpId").asText();
        CPState state = cpStates.get(cpId);
        if (state != null) {
            state.status = "AVERIADO";

            DatabaseManager.updateChargingPointStatus(cpId, "AVERIADO");
            DatabaseManager.logSystemEvent("CP_FAULT", cpId, "Punto reportado como averiado via Kafka");
        }
    }

    private static void handleHealthOk(JsonNode msg) {
        String cpId = msg.get("cpId").asText();
        CPState state = cpStates.get(cpId);
        if (state != null && "AVERIADO".equals(state.status)) {
            state.status = "ACTIVADO";

            DatabaseManager.updateChargingPointStatus(cpId,"ACTIVADO");
            DatabaseManager.logSystemEvent("CP_RECOVERED", cpId, "Punto recuperado de averia");
        }
    }

    private static void handleChargingEnd(JsonNode msg) {
        String cpId = msg.get("cpId").asText();
        String driverId = msg.get("driverId").asText();
        double totalConsumption = msg.get("totalConsumption").asDouble();
        double totalAmount = msg.get("totalAmount").asDouble();
        
        DatabaseManager.finishChargingSession(driverId, cpId, totalConsumption, totalAmount);
        DatabaseManager.updateChargingPointStatus(cpId, "ACTIVADO");
        DatabaseManager.logSystemEvent("CHARGING_ENDED", cpId, 
            "Carga finalizada - Consumo: " + totalConsumption + "kW, Importe: " + totalAmount + "€");
    }


    // --- Comandos de consola ---
    static class ScannerWrapper {
        static void startConsoleMenu() {
            java.util.Scanner sc = new java.util.Scanner(System.in);
            while (true) {
                System.out.println("\nComandos: STOP <cpId>, RESUME <cpId>, LIST, EXIT");
                String input = sc.nextLine().trim();
                if (input.isEmpty()) continue;

                String[] cmd = input.split(" ");
                switch (cmd[0].toUpperCase()) {
                    case "STOP":
                        if (cmd.length > 1) {
                            CPState s = cpStates.get(cmd[1]);
                            if (s != null) {
                                s.status = "PARADO";

                                DatabaseManager.updateChargingPointStatus(cmd[1], "PARADO");
                                DatabaseManager.logSystemEvent("CP_STOPPED", cmd[1], "Puesto fuera de servicio manualmente");

                                System.out.println("CP " + cmd[1] + " detenido.");
                                printPanel();
                            }
                        }
                        break;
                    case "RESUME":
                        if (cmd.length > 1) {
                            CPState s = cpStates.get(cmd[1]);
                            if (s != null && "PARADO".equals(s.status)) {
                                s.status = "ACTIVADO";

                                DatabaseManager.updateChargingPointStatus(cmd[1], "ACTIVADO");
                                DatabaseManager.logSystemEvent("CP_RESUMED", cmd[1], "Puesto reactivado manualmente");

                                System.out.println("CP " + cmd[1] + " reanudado.");
                                printPanel();
                            }
                        }
                        break;
                    case "LIST":
                        printPanel();
                        break;
                    case "BDSTATS":
                        printDatabaseStats();
                        break;
                    case "EXIT":
                        System.exit(0);
                    default:
                        System.out.println("Comando no reconocido.");
                }
            }
        }
    }

     private static void printDatabaseStats() {
        System.out.println("\n" + "=".repeat(50));
        System.out.println(" ESTADISTICAS BASE DE DATOS");
        System.out.println("=".repeat(50));
        
        try {
            var points = DatabaseManager.getAllChargingPoints();
            System.out.println("Puntos de recarga en BD: " + points.size());
            points.forEach(System.out::println);
            
        } catch (Exception e) {
            System.err.println("Error obteniendo estadisticas BD: " + e.getMessage());
        }
        System.out.println("=".repeat(50));
    }

    // --- Persistencia ---
    @SuppressWarnings("unchecked")
    private static void loadCPsFromFile() {
        try {
            if (new File(CPS_FILE).exists()) {
                JsonNode root = mapper.readTree(new File(CPS_FILE));
                for (JsonNode node : root) {
                    String cpId = node.get("cpId").asText();
                    String status = node.get("status").asText();
                    cpStates.put(cpId, new CPState(cpId, status));

                    DatabaseManager.updateChargingPointStatus(cpId, status);
                }
                System.out.println("Cargados " + cpStates.size() + " CPs desde " + CPS_FILE);
            }
        } catch (Exception e) {
            System.err.println("Error cargando CPs: " + e.getMessage());
        }
    }

    private static void saveCPsToFile() {
        try {
            ObjectNode root = mapper.createObjectNode();
            var array = root.arrayNode();
            for (CPState s : cpStates.values()) {
                var obj = mapper.createObjectNode();
                obj.put("cpId", s.cpId);
                obj.put("status", s.status);
                array.add(obj);
            }
            mapper.writerWithDefaultPrettyPrinter().writeValue(new File(CPS_FILE), array);
        } catch (Exception e) {
            System.err.println("Error guardando CPs: " + e.getMessage());
        }
    }

    // --- Panel de control ---
    private static void printPanel() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println(" PANEL DE CONTROL - EVCharging Network");
        System.out.println("=".repeat(80));
        if (cpStates.isEmpty()) {
            System.out.println("No hay puntos de recarga registrados.");
        } else {
            cpStates.values().forEach(System.out::println);
        }
        System.out.println("=".repeat(80) + "\n");
    }

    // --- Estado de un CP ---
    static class CPState {
        String cpId;
        String status; // DESCONECTADO, ACTIVADO, PARADO, SUMINISTRANDO, AVERIADO
        Double powerKw = 0.0;
        Double costEur = 0.0;
        String driverId = "";

        CPState(String cpId, String status) {
            this.cpId = cpId;
            this.status = status;
        }

        @Override
        public String toString() {
            String color = switch (status) {
                case "ACTIVADO" -> "🟢";
                case "PARADO" -> "🟠";
                case "SUMINISTRANDO" -> "🟢";
                case "AVERIADO" -> "🔴";
                default -> "⚪"; // DESCONECTADO
            };

            if ("SUMINISTRANDO".equals(status)) {
                return String.format("%s %s | Estado: %s | Conductor: %s | Potencia: %.1f kW | Coste: %.2f €",
                    color, cpId, status, driverId, powerKw, costEur);
            } else {
                String displayStatus = "PARADO".equals(status) ? "Fuera de Servicio" : status;
                return String.format("%s %s | Estado: %s", color, cpId, displayStatus);
            }
        }
    }
}