import java.rmi.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;

public class ChordUser {
    int port;

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

    public ChordUser(int p) {
        this.port = p;
    }

    public static void main(String args[]) {
        if (args.length < 1 ) throw new IllegalArgumentException("Parameter: <port>");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // TODO: Execute leave
        }));

        ChordUser chordUser = new ChordUser(Integer.parseInt(args[0]));
        Scanner in = new Scanner(System.in);

        Timer timer1 = new Timer();
        timer1.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    long guid = hash(Integer.toString(chordUser.port));
                    Chord chord = new Chord(chordUser.port, guid);

                    try {
                        Files.createDirectories(Paths.get(guid + "/repository"));
                    } catch(IOException e) {
                        e.printStackTrace();
                    }

                    System.out.printf("Usage:\n\tjoin <ip> <port>\n\twrite <file> (the file must be an integer stored in the working directory, i.e, ./%d/file\n", guid);
                    System.out.println("\tread <file>\n\tdelete <file>\n\tprint");

                    while (true) {
                        String[] tokens = in.nextLine().split("\\s+");
                        switch(tokens[0]) {
                            case "join":
                                if(tokens.length != 3)
                                    System.out.printf("Expected arguments <ip> and <port>, but received %d args\n", tokens.length - 1);

                                try {
                                    int port = Integer.parseInt(tokens[2]);
                                    chord.joinRing(tokens[1], port);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                                break;
                            case "print":
                                chord.print();
                                break;
                            case "write":
                                if(tokens.length != 2)
                                    System.out.printf("Expected argument <file>, but received %d args\n", tokens.length - 1);

                                try {
                                    String path;
                                    String fileName = tokens[1];
                                    long guidObject = hash(fileName);

                                    // Windows file system must use ".\\" for directory changes
                                    path = "./" + guid + "/" + fileName; // path to file
                                    FileStream file = new FileStream(path);

                                    ChordMessageInterface peer = chord.locateSuccessor(guidObject);
                                    peer.put(guidObject, file); // put file into ring
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                                break;
                            case "read":
                                if(tokens.length != 2)
                                    System.out.printf("Expected argument <file>, but received %d args\n", tokens.length - 1);

                                /*
                                TODO: READ
                                Obtain chord that's responsible for file ./guid/filename
                                filename = tokens[1]
                                guidObject = hash(filename)
                                peer = chord.locateSuccessor(guidObject)
                                stream = peer.get(guidObject)
                                Store content of stream in the local file you create
                                 */

                                break;
                            case "delete":
                                if(tokens.length != 2)
                                    System.out.printf("Expected argument <file>, but received %d args\n", tokens.length - 1);

                                /*
                                TODO: DELETE
                                peer = chord.locateSuccessor(guidObject);
                                where guidObject = hash(fileName)
                                Call peer.delete(guidObject)
                                 */

                                break;
                            default:
                                System.out.printf("%s is an invalid command\n", tokens[0]);
                        }
                    }
                }
                catch(RemoteException e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1000);
    }
}