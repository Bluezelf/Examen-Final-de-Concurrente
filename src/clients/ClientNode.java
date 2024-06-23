package clients;

import java.io.*;
import java.net.*;

public class ClientNode {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 12345;

    public static void main(String[] args) {
        try (Socket socket = new Socket(SERVER_ADDRESS, SERVER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Send a task to the server
            String task = "Count words in this text";
            out.println(task);
            System.out.println("Sent task: " + task);

            // Receive the result from the server
            String result = in.readLine();
            System.out.println("Received result: " + result);
        } catch (IOException e) {
            System.err.println("Error connecting to server: " + e.getMessage());
        }
    }
}

