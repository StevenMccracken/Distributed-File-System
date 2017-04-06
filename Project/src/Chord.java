import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.*;
import java.rmi.registry.*;
import java.util.*;
import java.io.*;
import java.util.stream.Stream;

/**
 * Class represents an object in a peer-to-peer file system ring
 */
public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface {
    public static final int M = 2;

    Registry registry; // RMI registry for lookup of remote objects
    ChordMessageInterface predecessor, successor;
    ChordMessageInterface[] finger; // Finger table for this Chord
    int nextFinger; // The closest finger to this Chord
    long guid; // Global unique identifier

    /**
     * Locates a specific Chord object
     * @param ip the specific ip address
     * @param port the specific port
     * @return the specific Chord object based on ip and port
     */
    public static ChordMessageInterface rmiChord(String ip, int port) {
        ChordMessageInterface chord = null;
        try {
            // Locates the remote object responsible for the ip and port
            Registry registry = LocateRegistry.getRegistry(ip, port);
            chord = (ChordMessageInterface)(registry.lookup("Chord"));
        } catch(RemoteException | NotBoundException e) {
            System.out.printf("Unable to locate remote Chord object for %s:%d -> %s\n", ip, port, e.getMessage());
        }
        return chord;
    }

    /**
     * Determines if key is bounded by key1 and key2
     * @param key the key to test
     * @param key1 should be the lower-bound key
     * @param key2 should be the upper-bound key
     * @return true if the key is strictly within key1 and key2
     */
    public Boolean isKeyInSemiCloseInterval(long key, long key1, long key2) {
        if (key1 < key2) return (key > key1 && key <= key2);
        else return (key > key1 || key <= key2);
    }

    /**
     * Determines if key is within key1 and key2 at all
     * @param key the key to test
     * @param key1 should be the lower-bound key
     * @param key2 should be the upper-bound key
     * @return true if the key is somewhere around key1 or key2
     */
    public Boolean isKeyInOpenInterval(long key, long key1, long key2) {
        if (key1 < key2) return (key > key1 && key < key2);
        else return (key > key1 || key < key2);
    }

    /**
     * Places a file in the file system ring
     * @param guidObject the global unique identifier of the file
     * @param stream the data of the file
     * @throws RemoteException
     */
    public void put(long guidObject, InputStream stream) throws RemoteException {
        // Writes the file data to this chord's repository with the name guidObject
        String fileName = String.format("%d/repository/%d", this.guid, guidObject);
        try {
            FileOutputStream output = new FileOutputStream(fileName);

            while (stream.available() > 0) output.write(stream.read());

            System.out.printf("Finished writing %d to %s\n", guidObject, fileName);
            output.close();
        } catch(IOException e) {
            System.out.printf("Failed to completely write %d to %s -> %s\n", guidObject, fileName, e.getMessage());
        }
    }

    /**
     * Gets a file from the file system ring
     * @param guidObject the global unique identifier of the file
     * @return the data of the file
     * @throws IOException
     */
    public InputStream get(long guidObject) throws IOException {
        String filePath = String.format("%d/repository/%d", this.guid, guidObject);
        return new FileStream(filePath);
    }

    /**
     * Removes a file from the file system ring
     * @param guidObject the global unique identifier of the file
     * @throws IOException
     */
    public void delete(long guidObject) throws IOException {
        String filePath = String.format("%d/repository/%d", this.guid, guidObject);
        Files.delete(Paths.get(filePath));
    }

    /**
     * Accessor for the global unique identifier of this chord
     * @return the global unique identifier of this chord
     * @throws RemoteException
     */
    public long getId() throws RemoteException {
        return guid;
    }

    /**
     * Returns the state of this chord
     * @return true if this chord is alive, otherwise throws an exception
     * @throws RemoteException
     */
    public boolean isAlive() throws RemoteException {
        return true;
    }

    /**
     * Accessor for the predecessor of this chord
     * @return the Chord object representing the predecessor of this chord
     * @throws RemoteException
     */
    public ChordMessageInterface getPredecessor() throws RemoteException {
        return predecessor;
    }

    /**
     * Gets the successor of a specific global unique identifier
     * @param key the global unique identifier key
     * @return the successor of the object at the key
     * @throws RemoteException
     */
    public ChordMessageInterface locateSuccessor(long key) throws RemoteException {
        if (key == this.guid)
            throw new IllegalArgumentException(String.format("Key %d is not distinct", guid));
        if (successor.getId() != this.guid) {
            if (isKeyInSemiCloseInterval(key, guid, successor.getId())) return successor;

            ChordMessageInterface j = closestPrecedingNode(key);
            if (j == null) return null;
            return j.locateSuccessor(key);
        }
        return successor;
    }

    /**
     * Gets the closest preceding node in the peer ring
     * @param key the global unique identifier key
     * @return the closest link to the key
     * @throws RemoteException
     */
    public ChordMessageInterface closestPrecedingNode(long key) throws RemoteException {
        // TODO: Implement more efficient algorithm
        return successor;
    }

    /**
     * Joins a new peer to this chord's peer system ring
     * @param ip the ip address of the new peer
     * @param port the port of the new peer
     * @throws RemoteException
     */
    public void joinRing(String ip, int port) throws RemoteException {
        try {
            System.out.printf("Get Registry for %s:%d\n", ip, port);

            // Get the joining peer's chord object
            Registry registry = LocateRegistry.getRegistry(ip, port);
            ChordMessageInterface chord = (ChordMessageInterface)(registry.lookup("Chord"));

            predecessor = null;
            successor = chord.locateSuccessor(this.getId()); // Make sure the chord is in the right place

            // Make sure the correct peers are responsible for the correct files in the updated ring
            notify(successor, false);

            System.out.println("Joined new peer to your ring!");
        }  catch(RemoteException | NotBoundException e) {
            System.out.printf("Can't connect %s:%d to your ring\n", ip, port);
            successor = this;
        }
    }

    /**
     * Update this chord's successor to be the correct finger
     */
    public void findingNextSuccessor() {
        successor = this;
        for (int i = 0; i < M; i++) {
            try {
                if (finger[i].isAlive()) successor = finger[i];
            } catch(RemoteException | NullPointerException e) {
                finger[i] = null;
            }
        }
    }

    /**
     * Makes sure the peer system is correctly distributed
     */
    public void stabilize() {
        try {
            if (successor != null) {
                ChordMessageInterface x = successor.getPredecessor();

                if (x != null && x.getId() != this.getId() && isKeyInOpenInterval(x.getId(), this.getId(), successor.getId()))
                    successor = x;
                if (successor.getId() != getId()) successor.notify(this, false);
            }
        } catch(RemoteException | NullPointerException e) {
            findingNextSuccessor();
        }
    }

    /**
     * Transfers guid keys to the correct peers to ensure the file system works efficiently
     * @param j the peer to transfer the files to
     * @param killChord if j is leaving the peer-to-peer ring
     * @throws RemoteException
     */
    public void notify(ChordMessageInterface j, boolean killChord) throws RemoteException {
        if (predecessor == null || (predecessor != null && isKeyInOpenInterval(j.getId(), predecessor.getId(), guid)))
            predecessor = j;

        // Transfer keys in the range [j,i) to j;
        String path = String.format("%d/repository", this.guid);
        File repo = new File(path);

        File[] files = repo.listFiles();
        for(File file : files) {
            if (!file.getName().matches("[0-9]+")) continue; // Ignores system files like .DS_store, for example
            long fileName = Long.parseLong(file.getName());
            if (killChord || isKeyInOpenInterval(fileName, this.guid, j.getId())) {
                try {
                    FileStream newFile = new FileStream(String.format("%s/%d", path, fileName));
                    j.put(fileName, newFile);
                    file.delete();
                } catch(IOException e) {
                    System.out.printf("Failed to transfer %d to %d -> %s\n", fileName, j.getId(), e.getMessage());
                }
            }
        }
    }

    /**
     * Ensures that finger table is correct to ensure system functions efficiently
     */
    public void fixFingers() {
        long id = guid;
        try {
            long nextId;
            if (nextFinger == 0) nextId = (this.getId() + (1 << nextFinger));
            else nextId = finger[nextFinger - 1].getId();

            finger[nextFinger] = locateSuccessor(nextId);

            if (finger[nextFinger].getId() == guid) finger[nextFinger] = null;
            else nextFinger = (nextFinger + 1) % M;
        }
        catch(RemoteException | NullPointerException e) {
            finger[nextFinger] = null;
        }
    }

    /**
     * Ensures that predecessor is correctly known to ensure system functions efficiently
     */
    public void checkPredecessor() {
        try {
            if (predecessor != null && !predecessor.isAlive()) predecessor = null;
        }
        catch(RemoteException e) {
            predecessor = null;
            System.out.println("Predecessor left");
            try {
                if (successor.getId() == this.guid) System.out.println("You're all alone now...");
            }
            catch(NullPointerException | RemoteException e2) {}
        }
    }

    /**
     * Constructor for the Chord class
     * @param port the unique, un-hashed identifier of this class
     * @param guid the unique, hashed identifier of this class
     * @throws RemoteException
     */
    public Chord(int port, long guid) throws RemoteException {
        finger = new ChordMessageInterface[M];
        Stream.of(finger).forEach(f -> f = null); // Initialize all fingers to null
        this.guid = guid;

        predecessor = null;
        successor = this;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                stabilize();
                fixFingers();
                checkPredecessor();
            }
        }, 500, 500);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            timer.cancel();
            timer.purge();
        }));

        // Create the registry and bind the name and object.
        System.out.printf("%d is starting RMI at port %d\n", guid, port);
        registry = LocateRegistry.createRegistry(port);
        registry.rebind("Chord", this);
    }

    /**
     * Attempts to safely remove this chord from the peer ring
     */
    public void leave() {
        try {
            if (this.successor != null && successor.getId() != this.guid)
                notify(this.successor, true);
        } catch(RemoteException e) {
            try {
                System.out.printf("Failed to successfully notify %d of all the files you were responsible for -> %s\n", this.successor.getId(), e.getMessage());
            } catch(RemoteException e2) {}
        }
    }

    /**
     * Prints the status of this chord to the console.
     * Status info contains this chord's predecessor, successor, and it's finger table
     */
    public void print() {
        try {
            System.out.printf("\nSuccessor: %s\n", successor == null ? "none" : Long.toString(successor.getId()));
            System.out.printf("Predecessor: %s\n", predecessor == null ? "none" : Long.toString(predecessor.getId()));
            if (finger != null) {
                System.out.println("--- Finger Table ---");
                for (int i = 0; i < M; i++) {
                    try {
                        System.out.printf("Finger %d: %d\n", i, finger[i].getId());
                    } catch(NullPointerException e) {
                        System.out.printf("Finger %d: null\n", i);
                    }
                }
            }
            System.out.println();
        } catch(RemoteException e) {
            System.out.printf("Can't connect to port -> %s\n", e.getMessage());
        }
    }
}