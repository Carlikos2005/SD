import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.Socket;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EV_CP_M {

    private static volatile boolean engineHealthy = true;
    private static String cpId = "";
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Uso: java EV_CP_M <central_host> <central_port> <kafka_bootstrap> <cpId>");
            System.exit(1);
        }

        String centralHost = args[0];
        int centralPort = Integer.parseInt(args[1]);
        String kafkaBootstrap = args[2];
        cpId = args[3];

        System.out.println("EV_CP_M iniciado");
        System.out.println("Conectando a EV_Central en " + centralHost + ":" + centralPort);
        System.out.println("Kafka: " + kafkaBootstrap);
        System.out.println("CP ID: " + cpId);

        // 1. Registro en EV_Central por socket
        boolean registered = registerWithCentral(centralHost, centralPort, cpId);
        if (!registered) {
            System.err.println("Fallo al registrarse en la Central");
            System.exit(1);
        }

        // 2. Configurar productor Kafka
        KafkaProducer<String, String> producer = createKafkaProducer(kafkaBootstrap);

        // Monitoreo automático del Engine
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            checkEngineHealth(producer, cpId);
        }, 0, 5, TimeUnit.SECONDS); // Verificar cada 5 segundos

        // 3. Interfaz de usuario para controles manuales
        System.out.println("\n" + "=".repeat(50));
        System.out.println(" MONITOR DEL PUNTO DE RECARGA - " + cpId);
        System.out.println("=".repeat(50));
        System.out.println("Estado actual: " + (engineHealthy ? "SALUDABLE" : "AVERIADO"));
        System.out.println("\nComandos manuales:");
        System.out.println("  f -> Simular avería manual");
        System.out.println("  r -> Simular recuperación manual"); 
        System.out.println("  s -> Estado actual del sistema");
        System.out.println("  q -> Salir");
        System.out.println("=".repeat(50));

        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print("\nComando > ");
            String input = sc.nextLine().trim().toLowerCase();
            
            switch (input) {
                case "f":
                    if (engineHealthy) {
                        engineHealthy = false;
                        sendFault(producer, cpId);
                        System.out.println("AVERIA MANUAL SIMULADA");
                    } else {
                        System.out.println("ERROR: El sistema YA está en estado de avería");
                    }
                    break;
                    
                case "r":
                    if (!engineHealthy) {
                        engineHealthy = true;
                        sendHealthOk(producer, cpId);
                        System.out.println("RECUPERACION MANUAL SIMULADA");
                    } else {
                        System.out.println("ERROR: El sistema YA está saludable");
                    }
                    break;
                    
                case "s":
                    printSystemStatus();
                    break;
                    
                case "q":
                    System.out.println("Cerrando monitor...");
                    scheduler.shutdown();
                    producer.close();
                    System.exit(0);
                    break;
                    
                default:
                    System.out.println("Comando no reconocido. Usa: f, r, s, q");
            }
        }
    }

    // Verificación automática del estado del Engine
    private static void checkEngineHealth(KafkaProducer<String, String> producer, String cpId) {
        boolean previousHealth = engineHealthy;
        
        // Simulación: 95% de probabilidad de estar saludable
        if (Math.random() > 0.95 && engineHealthy) {
            engineHealthy = false;
            sendFault(producer, cpId);
            System.out.println("MONITOR: Avería detectada automáticamente");
        }
        
        // Solo notificar cambios de estado
        if (previousHealth != engineHealthy) {
            System.out.println("MONITOR: Estado cambiado a: " + 
                (engineHealthy ? "SALUDABLE" : "AVERIADO"));
        }
    }

    // Mostrar estado del sistema
    private static void printSystemStatus() {
        System.out.println("\n--- ESTADO DEL SISTEMA ---");
        System.out.println("CP ID: " + cpId);
        System.out.println("Estado Engine: " + (engineHealthy ? "SALUDABLE" : "AVERIADO"));
        System.out.println("Monitor: ACTIVO");
        System.out.println("Última verificación: " + java.time.LocalTime.now());
        System.out.println("---------------------------");
    }

    private static boolean registerWithCentral(String host, int port, String cpId) {
        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("REGISTER#" + cpId);
            String response = in.readLine();
            if ("ACK".equals(response)) {
                System.out.println("Registro exitoso en EV_Central");
                return true;
            } else {
                System.err.println("Registro rechazado: " + response);
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error conectando con Central: " + e.getMessage());
            return false;
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    private static void sendFault(KafkaProducer<String, String> producer, String cpId) {
        sendMessage(producer, "FAULT", cpId, "Avería detectada por monitor");
    }

    private static void sendHealthOk(KafkaProducer<String, String> producer, String cpId) {
        sendMessage(producer, "HEALTH_OK", cpId, "Sistema recuperado");
    }

    private static void sendMessage(KafkaProducer<String, String> producer, String type, String cpId, String description) {
        try {
            ObjectNode msg = mapper.createObjectNode();
            msg.put("type", type);
            msg.put("cpId", cpId);
            msg.put("description", description);
            msg.put("timestamp", System.currentTimeMillis());
            
            String json = mapper.writeValueAsString(msg);
            producer.send(new ProducerRecord<>("evcharging", cpId, json));
            
            System.out.println("Enviado a Kafka: " + type + " - " + description);
        } catch (Exception e) {
            System.err.println("Error enviando mensaje a Kafka: " + e.getMessage());
        }
    }
}