// EV_CP_E.java
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class EV_CP_E {

    private static volatile boolean charging = false;
    private static volatile boolean faultMode = false;
    private static String currentDriverId = "";
    private static final Random rand = new Random();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Uso: java EV_CP_E <kafka_bootstrap> <cpId> <driverId>");
            System.exit(1);
        }

        String kafkaBootstrap = args[0];   // Ej: 192.168.1.10:9092
        String cpId = args[1];             // Ej: CP001
        currentDriverId = args[2];         // Ej: DRV123

        System.out.println("EV_CP_E (Engine) iniciado");
        System.out.println("Kafka: " + kafkaBootstrap);
        System.out.println("CP ID: " + cpId);
        System.out.println("Driver ID: " + currentDriverId);
        System.out.println("Esperando comandos...");

        KafkaProducer<String, String> producer = createKafkaProducer(kafkaBootstrap);

        // Hilo para telemetría (solo si está recargando)
        Thread telemetryThread = new Thread(() -> {
            ObjectMapper mapper = new ObjectMapper();
            while (!Thread.currentThread().isInterrupted()) {
                if (charging && !faultMode) {
                    try {
                        double power = 10.0 + rand.nextDouble() * 5.0; // 10-15 kW
                        double cost = power * 0.20; // €/kWh = 0.20

                        ObjectNode msg = mapper.createObjectNode();
                        msg.put("type", "TELEMETRY");
                        msg.put("cpId", cpId);
                        msg.put("powerKw", power);
                        msg.put("costEur", cost);
                        msg.put("driverId", currentDriverId);

                        producer.send(new ProducerRecord<>("evcharging", cpId, msg.toString()));
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
        telemetryThread.start();

        // Menú de control
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.println("\nComandos:");
            System.out.println("  start  → Iniciar recarga");
            System.out.println("  stop   → Detener recarga");
            System.out.println("  fault  → Simular avería (KO al Monitor)");
            System.out.println("  ok     → Salir de modo avería");
            System.out.println("  quit   → Salir");

            String cmd = sc.nextLine().trim().toLowerCase();

            switch (cmd) {
                case "start":
                    charging = true;
                    System.out.println("Recarga iniciada");
                    break;
                case "stop":
                    charging = false;
                    System.out.println("Recarga detenida");
                    break;
                case "fault":
                    faultMode = true;
                    charging = false;
                    System.out.println("Avería simulada (enviará KO al Monitor)");
                    break;
                case "ok":
                    faultMode = false;
                    System.out.println("Modo avería desactivado");
                    break;
                case "quit":
                    telemetryThread.interrupt();
                    producer.close();
                    System.exit(0);
                default:
                    System.out.println("Comando no reconocido.");
            }
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    // Método que el Monitor (EV_CP_M) llamaría por socket para comprobar salud
    // En esta versión simplificada, se simula con el menú "fault" / "ok"
    // Pero en una versión avanzada, podrías abrir un ServerSocket aquí
    public static boolean isHealthy() {
        return !faultMode;
    }
}