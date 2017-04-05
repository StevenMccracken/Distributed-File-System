import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.*;
import java.rmi.registry.*;
import java.util.*;
import java.io.*;
import java.util.stream.Stream;

public class Chord extends java.rmi.server.UnicastRemoteObject implements ChordMessageInterface {
    public static final int M = 2;

    Registry registry; // RMI registry for lookup of remote objects
    ChordMessageInterface predecessor, successor;
    ChordMessageInterface[] finger;
    int nextFinger;
    long guid;

    /**
     * Locates a specific Chord object
     * @param ip the specific ip address
     * @param port the specific port
     * @return the specific Chord object based on ip and port
     */
    public static ChordMessageInterface rmiChord(String ip, int port) {
        ChordMessageInterface chord = null;
        try {
            Registry registry = LocateRegistry.getRegistry(ip, port);
            chord = (ChordMessageInterface)(registry.lookup("Chord"));
        } catch (RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
        return chord;
    }

    /**
     *
     * @param key
     * @param key1
     * @param key2
     * @return
     */
    public Boolean isKeyInSemiCloseInterval(long key, long key1, long key2) {
        if (key1 < key2) return (key > key1 && key <= key2);
        else return (key > key1 || key <= key2);
    }

    /**
     *
     * @param key
     * @param key1
     * @param key2
     * @return
     */
    public Boolean isKeyInOpenInterval(long key, long key1, long key2) {
        if (key1 < key2) return (key > key1 && key < key2);
        else return (key > key1 || key < key2);
    }

    /**
     *
     * @param guid_object
     * @param stream
     * @throws RemoteException
     */
    public void put(long guid_object, InputStream stream) throws RemoteException {
        try {
            String fileName = String.format("%d/repository/%d", this.guid, guid_object);
            FileOutputStream output = new FileOutputStream(fileName);

            while (stream.available() > 0) output.write(stream.read());

            System.out.printf("Finished writing %d for %d\n", guid_object, this.guid);
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param guidObject
     * @return
     * @throws IOException
     */
    public InputStream get(long guidObject) throws IOException {
        String filePath = String.format("%d/repository/%d", this.guid, guidObject);
        return new FileStream(filePath);
    }

    /**
     *
     * @param guidObject
     * @throws IOException
     */
    public void delete(long guidObject) throws IOException {
        String filePath = String.format("%d/repository/%d", this.guid, guidObject);
        Files.delete(Paths.get(filePath));
    }

    /**
     *
     * @return
     * @throws RemoteException
     */
    public long getId() throws RemoteException {
        return guid;
    }

    /**
     *
     * @return
     * @throws RemoteException
     */
    public boolean isAlive() throws RemoteException {
        return true;
    }

    /**
     *
     * @return
     * @throws RemoteException
     */
    public ChordMessageInterface getPredecessor() throws RemoteException {
        return predecessor;
    }

    /**
     *
     * @param key
     * @return
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
     *
     * @param key
     * @return
     * @throws RemoteException
     */
    public ChordMessageInterface closestPrecedingNode(long key) throws RemoteException {
        // TODO: Implement more efficient algorithm
        return successor;
    }

    /**
     *
     * @param ip
     * @param port
     * @throws RemoteException
     */
    public void joinRing(String ip, int port) throws RemoteException {
        try {
            System.out.println("Get Registry to join ring");

            Registry registry = LocateRegistry.getRegistry(ip, port);
            ChordMessageInterface chord = (ChordMessageInterface)(registry.lookup("Chord"));

            predecessor = null;
            successor = chord.locateSuccessor(this.getId());

            notify(successor, false);

            System.out.println("Joined ring!");
        }  catch(RemoteException | NotBoundException e) {
            System.out.printf("Can't connect to %s:%d\n", ip, port);
            successor = this;
        }
    }

    /**
     *
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
     *
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
     *
     * @param j
     * @param killChord
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
            if (!file.getName().matches("[0-9]+")) continue;
            long file_name = Long.parseLong(file.getName());
            if (killChord || isKeyInOpenInterval(file_name, this.guid, j.getId())) {
                try {
                    FileStream new_file = new FileStream(String.format("%s/%d", path, file_name));
                    j.put(file_name, new_file);
                    boolean deleted = file.delete();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
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
//            e.printStackTrace();
        }
    }

    /**
     *
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
     *
     * @param port
     * @param guid
     * @throws RemoteException
     */
    public Chord(int port, long guid) throws RemoteException {
        finger = new ChordMessageInterface[M];
        Stream.of(finger).forEach(f -> f = null);
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

        // Create the registry and bind the name and object.
        System.out.printf("%d is starting RMI at port %d\n", guid, port);
        registry = LocateRegistry.createRegistry(port);
        registry.rebind("Chord", this);
    }

    /**
     *
     */
    public void leave() {
        // TODO: Re-adjust algorithm
        try {
            if (this.successor != null && successor.getId() != this.guid)
                notify(this.successor, true);
        } catch(RemoteException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     */
    public void print() {
        try {
            System.out.printf("\nSuccessor: %s\n", successor == null ? "none" : Long.toString(successor.getId()));
            System.out.printf("Predecessor: %s\n", predecessor == null ? "none" : Long.toString(predecessor.getId()));
            if(finger != null) {
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