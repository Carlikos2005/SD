// TestClient.java
import java.io.*;
import java.net.Socket;

public class TestClient {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Uso: java TestClient <host> <puerto> <mensaje>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String message = args[2];

        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            System.out.println("Enviando: " + message);
            out.println(message);
            String response = in.readLine();
            System.out.println("Respuesta: " + response);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}