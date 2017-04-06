# CECS 327
Course Title: Introduction to Networks and Distributed Computing

## Assignment - Peer-to-peer File System

## How to run
1) ```cd P2P-File-System/Project/out/production/Project```
2) ```javac -d ./ ../../../src/*.java```
3) ```java ChordUser {port}```

### In order to add files to the system, they must exist within the working directory of the folder created for {port} in P2P-File-System/Project/out/production/Project/
### The working directory for any {port} will be created when starting the program correctly with {port}, and it will have a 19-digit integer name
### Start another console window or tab and follow the same steps to run, then perform the following commands in the program (where {ip}:{port} is running in another console process)
```join {ip} {port}```

