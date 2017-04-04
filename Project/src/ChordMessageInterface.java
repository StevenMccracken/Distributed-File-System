import java.rmi.*;
import java.io.*;

interface ChordMessageInterface extends Remote {
    ChordMessageInterface getPredecessor()                  throws RemoteException;
    ChordMessageInterface locateSuccessor(long key)         throws RemoteException;
    ChordMessageInterface closestPrecedingNode(long key)    throws RemoteException;
    void joinRing(String Ip, int port)                      throws RemoteException;
    void notify(ChordMessageInterface j, boolean killChord) throws RemoteException;
    boolean isAlive()                                       throws RemoteException;
    long getId()                                            throws RemoteException;

    void put(long guidObject, InputStream file) throws IOException;
    InputStream get(long guidObject)            throws IOException;
    void delete(long guidObject)                throws IOException;
}