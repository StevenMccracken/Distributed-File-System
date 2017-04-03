import java.rmi.*;
import java.net.*;
import java.util.*;
import java.io.*;
import java.math.BigInteger;
import java.security.*;
import java.nio.file.*;

public class ChordUser {
    int port;

    private long md5(String objectName) {
        try {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(objectName.getBytes());

            BigInteger bigInt = new BigInteger(1,m.digest());
            return Math.abs(bigInt.longValue());
        }
        catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public ChordUser(int p) {
        port = p;

        Timer timer1 = new Timer();
        timer1.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    long guid = md5("" + port);
                    Chord chord = new Chord(port, guid);

                    try {
                        Files.createDirectories(Paths.get(guid+"/repository"));
                    } catch(IOException e) {
                        e.printStackTrace();
                    }

                    System.out.println("Usage: \n\tjoin <ip> <port>\n\twrite <file> (the file must be an integer stored in the working directory, i.e, ./"+guid+"/file");
                    System.out.println("\tread <file>\n\tdelete <file>\n\tprint");

                    Scanner scan= new Scanner(System.in);
                    String delims = "[ ]+";
                    String command = "";

                    while (true) {
                        String text = scan.nextLine();
                        String[] tokens = text.split(delims);
                        if (tokens[0].equals("join") && tokens.length == 3) {
                            try {
                                int portToConnect = Integer.parseInt(tokens[2]);
                                chord.joinRing(tokens[1], portToConnect);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (tokens[0].equals("print")) chord.Print();
                        if (tokens[0].equals("write") && tokens.length == 2) {
                            try {
                                String path;
                                String fileName = tokens[1];
                                long guidObject = md5(fileName);

                                // If you are using windows you have to use path = ".\\"+  guid +"\\"+fileName; // path to file
                                path = "./"+  guid +"/"+fileName; // path to file
                                FileStream file = new FileStream(path);

                                ChordMessageInterface peer = chord.locateSuccessor(guidObject);
                                peer.put(guidObject, file); // put file into ring
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (tokens[0].equals("read") && tokens.length == 2) {
                            //TODO: Create a local
                            //    "./"+  guid +"/"+fileName
                            // where filename = tokens[1];
                            // Obtain the chord that is responsable for the file:
                            //  peer = chord.locateSuccessor(guidObject);
                            // where guidObject = md5(fileName);
                            // Now you can obtain the conted of the file in the chord using
                            // Call stream = peer.get(guidObject)
                            // Store the content of stream in the file that you create
                        }
                        if (tokens[0].equals("delete") && tokens.length == 2) {
                            // Obtain the chord that is responsable for the file:
                            //  peer = chord.locateSuccessor(guidObject);
                            // where guidObject = md5(fileName)
                            // Call peer.delete(guidObject)
                        }
                    }
                }
                catch(RemoteException e) {
                    e.printStackTrace();
                }
            }
        }, 1000, 1000);
    }

    public static void main(String args[]) {
        if (args.length < 1 )
            throw new IllegalArgumentException("Parameter: <port>");
        try {
            ChordUser chordUser = new ChordUser(Integer.parseInt(args[0]));
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}