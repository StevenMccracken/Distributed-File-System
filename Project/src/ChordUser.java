import java.rmi.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;

public class ChordUser {
    private int port;
    private long guid;
    private Chord chord;

    public ChordUser(int port) {
        this.port = port;
        this.guid = hash(Integer.toString(port));
    }

    public boolean join(String[] input) {
        if(input.length < 3) {
            System.out.printf("Expected arguments <ip> and <port>, but received %d %s\n", input.length - 1, input.length == 2 ? "arg" : "args");
            return false;
        }

        int port = Integer.parseInt(input[2]);
        try {
            chord.joinRing(input[1], port);
        } catch(RemoteException e) {
            return false;
        }
        return true;
    }

    public boolean write(String[] input) {
        if(input.length != 2) {
            System.out.printf("Expected argument <file>, but received %d args\n", input.length - 1);
            return false;
        }

        try {
            String fileName = input[1];
            long guidObject = hash(fileName);
            ChordMessageInterface peer = chord.locateSuccessor(guidObject);

            String path = String.format("./%d/%s", guid, fileName);
            FileStream file = new FileStream(path);
            if(peer != null) peer.put(guidObject, file); // put file into ring
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

    public boolean read(String[] input) {
        if(input.length != 2) {
            System.out.printf("Expected argument <file>, but received %d args\n", input.length - 1);
            return false;
        }

        try {
            String fileName = input[1], homeDir = System.getProperty("user.home");
            long guidObject = hash(fileName);

            Files.createDirectories(Paths.get(String.format("%s/temp", homeDir)));
            String savePath = String.format("%s/temp/%s", homeDir, fileName);

            ChordMessageInterface peer = chord.locateSuccessor(guidObject);
            InputStream fileStream = peer.get(guidObject);

            FileOutputStream writeStream = new FileOutputStream(savePath);
            while (fileStream.available() > 0)
                writeStream.write(fileStream.read());

            System.out.printf("Finished downloading %s to %s\n", fileName, savePath);
        } catch(IOException e) {
            if(e instanceof RemoteException) e.printStackTrace();
            else System.out.println("That file does not exist");
            return false;
        }
        return true;
    }

    public boolean delete(String[] input) {
        if(input.length != 2) {
            System.out.printf("Expected argument <file>, but received %d args\n", input.length - 1);
            return false;
        }

        try {
            String fileName = input[1];
            long guidObject = hash(fileName);

            ChordMessageInterface peer = chord.locateSuccessor(guidObject);
            if(peer != null) peer.delete(guidObject);
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

    public void quit() {
        if(chord != null) chord.kill();
    }

    public void run() {
        Scanner in = new Scanner(System.in);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    chord = new Chord(port, guid);

                    try {
                        Files.createDirectories(Paths.get(String.format("%d/repository", guid)));
                    } catch(IOException e) {
                        e.printStackTrace();
                    }

                    System.out.println("----- Peer-to-peer File System -----");
                    while (true) {
                        System.out.printf("Options\n\tjoin <ip> <port>\n\twrite <file>\n\tread <file>\n\tdelete <file>\n\tprint\n\tleave\n$ ", guid);
                        String[] input = in.nextLine().split("\\s+");
                        switch(input[0]) {
                            case "join":
                                boolean joined = join(input);
                                break;
                            case "print":
                                chord.print();
                                break;
                            case "write":
                                boolean written = write(input);
                                break;
                            case "read":
                                boolean read = read(input);
                                break;
                            case "delete":
                                boolean deleted = delete(input);
                                break;
                            case "leave":
                                System.exit(1);
                                break;
                            default:
                                System.out.printf("%s is an invalid command\n", input[0]);
                        }
                    }
                }
                catch(RemoteException e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1000);
    }

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

    public static void main(String args[]) throws IOException {
        if (args.length < 1 ) throw new IllegalArgumentException("Parameter: <port>");
        int port = Integer.parseInt(args[0]);
        ChordUser chordUser = new ChordUser(port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> chordUser.quit()));
        chordUser.run();
    }
}