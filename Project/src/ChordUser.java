import java.rmi.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;
import java.util.stream.Stream;

public class ChordUser {
    private int port;
    private long guid;
    private Chord chord;

    /**
     * Constructor for the ChordUser class
     * @param port the port that the user is at
     */
    public ChordUser(int port) {
        this.port = port;
        this.guid = hash(Integer.toString(port));
    }

    /**
     * Attempts to put a user into the ring of peers
     * @param input the user-entered tokens containing their ip address and port
     * @return true if the user successfully joined the system, and false otherwise
     */
    public boolean join(String[] input) {
        // Verify user input
        if(input.length < 3) {
            System.out.printf("Expected arguments <ip> and <port>, but received %d %s\n", input.length - 1, input.length == 2 ? "arg" : "args");
            return false;
        }

        int port = Integer.parseInt(input[2]);
        try {
            chord.joinRing(input[1], port); // Join the user-requested user to this user's peer system
        } catch(RemoteException e) {
            System.out.printf("Unable to join user %d ->%s\n", port, e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Attempts to add a file to the file system
     * @param input the user-entered tokens containing the name of the file
     * @return true if the file was successfully added to the system, and false otherwise
     */
    public boolean write(String[] input) {
        // Verify user input
        if(input.length != 2) {
            System.out.printf("Expected argument <file>, but received %d args\n", input.length - 1);
            return false;
        }

        try {
            String fileName = input[1];
            long guidObject = hash(fileName);
            String path = String.format("%d/%s", guid, fileName);

            // Find the peer responsible for hosting the user-requested file
            ChordMessageInterface peer = chord.locateSuccessor(guidObject);
            FileStream file = new FileStream(path);
            if(peer != null) peer.put(guidObject, file); // Put the file into the ring
            else {
                System.out.println("Unable to write file because of node corruption");
                return false;
            }
        } catch(IllegalArgumentException e) {
            System.out.printf("%s can't be created because it has the same id as your port\n", input[1]);
            return false;
        } catch(RemoteException e) {
            System.out.println(e);
            return false;
        } catch(IOException e) {
            System.out.printf("%s does not exist. It must exist in your working directory (/%d/repository/)\n", input[1], guid);
            return false;
        }
        return true;
    }

    /**
     * Attempts to read a file from the file system
     * @param input the user-entered tokens containing the name of the file
     * @return true if the file was successfully found and saved locally, and false otherwise
     */
    public boolean read(String[] input) {
        // Verify user input
        if(input.length != 2) {
            System.out.printf("Expected argument <file>, but received %d args\n", input.length - 1);
            return false;
        }

        try {
            String fileName = input[1], homeDir = System.getProperty("user.home");
            long guidObject = hash(fileName);

            // Save the user-requested file to their working directory
            String savePath = String.format("%d/%s", this.guid, fileName);

            // Find the peer responsible for the user-requested file
            ChordMessageInterface peer = chord.locateSuccessor(guidObject);
            if (peer == null) {
                System.out.println("Unable to read file because of node corruption");
                return false;
            }

            // Write the downloaded file to the local file
            InputStream fileStream = peer.get(guidObject);
            FileOutputStream writeStream = new FileOutputStream(savePath);
            while (fileStream.available() > 0)
                writeStream.write(fileStream.read());

            System.out.printf("Finished downloading %s to %s\n", fileName, savePath);

            Scanner in = new Scanner(System.in);
            System.out.println("Would you like to print the contents of the file here? (yes or no)");
            if(in.nextLine().toLowerCase().equals("yes")) {
                System.out.println("-----------------------------------");
                try (Stream<String> stream = Files.lines(Paths.get(savePath))) {
                    stream.forEach(System.out::println);
                } catch (IOException e) {
                    System.out.printf("There was an error reading the file -> %s\n", e.getMessage());
                }
                System.out.println("-----------------------------------");
            }
        } catch(IOException e) {
            if(e instanceof RemoteException) e.printStackTrace();
            else System.out.println("That file does not exist");
            return false;
        }
        return true;
    }

    /**
     * Attempts to delete a file from the file system
     * @param input the user-entered tokens containing the name of the file
     * @return true if the file was successfully deleted, and false otherwise
     */
    public boolean delete(String[] input) {
        // Verify user input
        if(input.length != 2) {
            System.out.printf("Expected argument <file>, but received %d args\n", input.length - 1);
            return false;
        }

        try {
            String fileName = input[1];
            long guidObject = hash(fileName);

            // Find the peer responsible for the user-requested file
            ChordMessageInterface peer = chord.locateSuccessor(guidObject);
            if(peer != null) peer.delete(guidObject); // If we found the peer, try and delete the file
            else {
                System.out.println("Unable to delete file because of node corruption");
                return false;
            }
        } catch(IllegalArgumentException e) {
            System.out.printf("%s can't exist because it has the same id as someone's port\n", input[1]);
            return false;
        } catch(RemoteException e) {
            System.out.println(e);
            return false;
        } catch(IOException e) {
            System.out.printf("%s doesn't exist\n", e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Pushes the user out of the file system
     */
    public void quit() {
        // Try to method to transfer files from this user to other peers
        if(chord != null) chord.leave();
    }

    /**
     * Runs the program for the user
     */
    public void run() {
        Scanner in = new Scanner(System.in);
        Timer timer = new Timer();

        // Start the main program thread
        timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {

                // Attempt to create registry and create local directory to correctly run the program
                try {
                    chord = new Chord(port, guid);
                    Files.createDirectories(Paths.get(String.format("%d/repository", guid)));
                } catch(RemoteException e) {
                    System.out.printf("Unable to connect to file system -> %s\n", e.getMessage());
                    System.exit(-1);
                } catch(IOException e) {
                    System.out.printf("Unable to create local file system directory -> %s\n", e.getMessage());
                    System.exit(-1);
                }

                // Print menu options to user and take user input until they leave normally or abruptly
                System.out.println("----- Peer-to-peer File System -----");
                while (true) {
                    System.out.print("Options\n\tjoin <ip> <port>\n\twrite <file>\n\tread <file>\n\tdelete <file>\n\tprint\n\tleave\n$ ");
                    String[] input = in.nextLine().split("\\s+"); // Get user input tokens
                    switch(input[0]) {
                        case "join":
                            join(input);
                            break;
                        case "print":
                            chord.print();
                            break;
                        case "write":
                            write(input);
                            break;
                        case "read":
                            read(input);
                            break;
                        case "delete":
                            delete(input);
                            break;
                        case "leave":
                            System.exit(1);
                            break;
                        default:
                            System.out.printf("%s is an invalid command\n", input[0]);
                    }
                }
            }
        }, 1000, 1000);
    }

    /**
     * One-way hashing algorithm for the port of the user
     * @param objectName the port of the user
     * @return the hashed value of the user's port
     */
    public static long hash(String objectName) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());

            BigInteger bigInt = new BigInteger(1, m.digest());
            return Math.abs(bigInt.longValue());
        }
        catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Main method
     * @param args the user's port
     */
    public static void main(String args[]) {
        if (args.length < 1 ) throw new IllegalArgumentException("Parameter: <port>");
        int port = Integer.parseInt(args[0]);
        ChordUser chordUser = new ChordUser(port);

        // Add catch for when the user abruptly ends the program
        Runtime.getRuntime().addShutdownHook(new Thread(() -> chordUser.quit()));
        chordUser.run(); // Start the program for the user
    }
}