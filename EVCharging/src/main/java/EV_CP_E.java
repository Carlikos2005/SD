// EV_CP_E.java
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class EV_CP_E {

    private static volatile boolean charging = false;
    private static volatile boolean faultMode = false;
    private static volatile boolean authorized = false;
    private static String currentDriverId = "";
    private static String cpId = "";
    private static final Random rand = new Random();
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Uso: java EV_CP_E <kafka_bootstrap> <cpId> <driverId>");
            System.exit(1);
        }

        String kafkaBootstrap = args[0];
        cpId = args[1];
        currentDriverId = args[2];

        System.out.println("EV_CP_E (Engine) iniciado");
        System.out.println("Kafka: " + kafkaBootstrap);
        System.out.println("CP ID: " + cpId);
        System.out.println("Driver ID: " + currentDriverId);

        KafkaProducer<String, String> producer = createKafkaProducer(kafkaBootstrap);
        
        // Consumir autorizaciones de la Central
        KafkaConsumer<String, String> consumer = createKafkaConsumer(kafkaBootstrap, cpId);
        
        // Registrar CP en la Central
        registerWithCentral(producer, cpId);

        // Hilo para escuchar autorizaciones
        Thread authThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode msg = mapper.readTree(record.value());
                        String type = msg.get("type").asText();
                        
                        if ("AUTHORIZE_CHARGING".equals(type) && cpId.equals(msg.get("cpId").asText())) {
                            authorized = true;
                            System.out.println("AUTORIZACIÓN RECIBIDA - Puede iniciar carga");
                        } else if ("STOP_CHARGING".equals(type) && cpId.equals(msg.get("cpId").asText())) {
                            charging = false;
                            authorized = false;
                            System.out.println("ORDEN DE STOP RECIBIDA - Deteniendo carga");
                        }
                    } catch (Exception e) {
                        System.err.println("Error procesando mensaje: " + e.getMessage());
                    }
                }
            }
        });
        authThread.start();

        // Hilo para telemetría
        Thread telemetryThread = new Thread(() -> {
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
            System.out.println("  start  → Iniciar recarga " + (authorized ? "SI" : "NO (Esperando autorización)"));
            System.out.println("  stop   → Detener recarga");
            System.out.println("  end    → Finalizar sesión de carga");
            System.out.println("  fault  → Simular avería (KO al Monitor)");
            System.out.println("  ok     → Salir de modo avería");
            System.out.println("  quit   → Salir");

            String cmd = sc.nextLine().trim().toLowerCase();

            switch (cmd) {
                case "start":
                    if (authorized && !faultMode) {
                        charging = true;
                        // Notificar inicio de carga
                        notifyChargingStart(producer, cpId, currentDriverId);
                        System.out.println("Recarga iniciada para conductor: " + currentDriverId);
                    } else if (!authorized) {
                        System.out.println("Esperando autorización de la Central");
                    } else {
                        System.out.println("No se puede iniciar - CP en modo avería");
                    }
                    break;
                    
                case "stop":
                    charging = false;
                    System.out.println("Recarga pausada");
                    break;
                    
                case "end":
                    charging = false;
                    authorized = false;
                    // Notificar fin de carga con totales
                    notifyChargingEnd(producer, cpId, currentDriverId);
                    System.out.println("Sesión de carga finalizada");
                    break;
                    
                case "fault":
                    faultMode = true;
                    charging = false;
                    // Notificar avería
                    notifyFault(producer, cpId);
                    System.out.println("Avería simulada (enviará KO al Monitor)");
                    break;
                    
                case "ok":
                    faultMode = false;
                    // Notificar recuperación
                    notifyHealthOk(producer, cpId);
                    System.out.println("Modo avería desactivado");
                    break;
                    
                case "quit":
                    telemetryThread.interrupt();
                    authThread.interrupt();
                    producer.close();
                    consumer.close();
                    System.exit(0);
                    
                default:
                    System.out.println("Comando no reconocido.");
            }
        }
    }

    // Métodos para notificar eventos a la Central
    private static void registerWithCentral(KafkaProducer<String, String> producer, String cpId) {
        try {
            ObjectNode msg = mapper.createObjectNode();
            msg.put("type", "CP_REGISTRATION");
            msg.put("cpId", cpId);
            msg.put("location", "Ubicación_" + cpId);
            producer.send(new ProducerRecord<>("evcharging", cpId, msg.toString()));
            System.out.println("CP registrado en Central");
        } catch (Exception e) {
            System.err.println("Error registrando CP: " + e.getMessage());
        }
    }

    private static void notifyChargingStart(KafkaProducer<String, String> producer, String cpId, String driverId) {
        try {
            ObjectNode msg = mapper.createObjectNode();
            msg.put("type", "CHARGING_START");
            msg.put("cpId", cpId);
            msg.put("driverId", driverId);
            producer.send(new ProducerRecord<>("evcharging", cpId, msg.toString()));
        } catch (Exception e) {
            System.err.println("Error notificando inicio carga: " + e.getMessage());
        }
    }

    private static void notifyChargingEnd(KafkaProducer<String, String> producer, String cpId, String driverId) {
        try {
            ObjectNode msg = mapper.createObjectNode();
            msg.put("type", "CHARGING_END");
            msg.put("cpId", cpId);
            msg.put("driverId", driverId);
            msg.put("totalConsumption", 15.5 + rand.nextDouble() * 10.0); // Simulado
            msg.put("totalAmount", 3.1 + rand.nextDouble() * 2.0); // Simulado
            producer.send(new ProducerRecord<>("evcharging", cpId, msg.toString()));
        } catch (Exception e) {
            System.err.println("Error notificando fin carga: " + e.getMessage());
        }
    }

    private static void notifyFault(KafkaProducer<String, String> producer, String cpId) {
        try {
            ObjectNode msg = mapper.createObjectNode();
            msg.put("type", "FAULT");
            msg.put("cpId", cpId);
            producer.send(new ProducerRecord<>("evcharging", cpId, msg.toString()));
        } catch (Exception e) {
            System.err.println("Error notificando avería: " + e.getMessage());
        }
    }

    private static void notifyHealthOk(KafkaProducer<String, String> producer, String cpId) {
        try {
            ObjectNode msg = mapper.createObjectNode();
            msg.put("type", "HEALTH_OK");
            msg.put("cpId", cpId);
            producer.send(new ProducerRecord<>("evcharging", cpId, msg.toString()));
        } catch (Exception e) {
            System.err.println("Error notificando recuperación: " + e.getMessage());
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    // Consumidor para recibir autorizaciones
    private static KafkaConsumer<String, String> createKafkaConsumer(String bootstrap, String cpId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cpe-group-" + cpId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("cp-commands"));
        return consumer;
    }

    public static boolean isHealthy() {
        return !faultMode;
    }
}