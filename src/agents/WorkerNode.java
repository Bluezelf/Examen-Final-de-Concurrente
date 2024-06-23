package agents;

import java.io.*;
import java.net.*;

public class WorkerNode {
    private static final int PORT = 12345;

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("WorkerNode started on port " + PORT);

            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                     PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                    String task = in.readLine();
                    System.out.println("Received task: " + task);

                    // Process the task (for now, just echo it back)
                    String result = "Processed: " + task;
                    out.println(result);
                } catch (IOException e) {
                    System.err.println("Error processing client request: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not start server: " + e.getMessage());
        }
    }
}

