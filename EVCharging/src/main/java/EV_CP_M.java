// EV_CP_M.java
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

public class EV_CP_M {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Uso: java EV_CP_M <central_host> <central_port> <kafka_bootstrap> <cpId>");
            System.exit(1);
        }

        String centralHost = args[0];      // IP de PC1
        int centralPort = Integer.parseInt(args[1]);
        String kafkaBootstrap = args[2];   // IP:9092 de PC1
        String cpId = args[3];

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

        // 3. Simular monitorización del Engine (EV_CP_E)
        System.out.println("Iniciando monitorización del Engine...");
        System.out.println("Pulsa 'f' para simular avería, 'r' para recuperación, 'q' para salir.");

        Scanner sc = new Scanner(System.in);
        boolean healthy = true;

        while (true) {
            String input = sc.nextLine().trim().toLowerCase();
            if ("f".equals(input) && healthy) {
                // Simular avería
                sendFault(producer, cpId);
                healthy = false;
            } else if ("r".equals(input) && !healthy) {
                // Simular recuperación
                sendHealthOk(producer, cpId);
                healthy = true;
            } else if ("q".equals(input)) {
                break;
            }
        }

        producer.close();
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
            e.printStackTrace();
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
        sendMessage(producer, "FAULT", cpId);
    }

    private static void sendHealthOk(KafkaProducer<String, String> producer, String cpId) {
        sendMessage(producer, "HEALTH_OK", cpId);
    }

    private static void sendMessage(KafkaProducer<String, String> producer, String type, String cpId) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode msg = mapper.createObjectNode();
            msg.put("type", type);
            msg.put("cpId", cpId);
            String json = mapper.writeValueAsString(msg);

            producer.send(new ProducerRecord<>("evcharging", cpId, json));
            System.out.println("Enviado a Kafka: " + json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}