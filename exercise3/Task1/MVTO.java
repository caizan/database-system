import java.util.*;

/**
 *  implement a (main-memory) data store with MVTO.
 *  objects are <int, int> key-value pairs.
 *  if an operation is to be refused by the MVTO protocol,
 *  undo its xact (what work does this take?) and throw an exception.
 *  garbage collection of versions is not required.
 *  Throw exceptions when necessary, such as when we try to execute an operation in
 *  a transaction that is not running; when we insert an object with an existing
 *  key; when we try to read or write a nonexisting key, etc.
 *  Keep the interface, we want to test automatically!
 *
 *  For my implementation I choose not to rollback ReadTimeStamp, as it can cause non relevant cascading rollbacks.
 *  I also decided 
 **/


public class MVTO {
  /* TODO -- your versioned key-value store data structure */

  private static HashMap<Integer, DatabaseObject> store = new HashMap<>();
  private static ArrayList<Integer> runningTransactions = new ArrayList<>();
  private static HashMap<Integer, ArrayList<Integer>> dependencies = new HashMap<>();
  private static int max_xact = 0;
  private static ArrayList<Integer> toCommit = new ArrayList<>();
  private static ArrayList<Integer> toRollback = new ArrayList<>();
  private static HashMap<Integer, ArrayList<ArrayList<Integer>>> writtenAndInsertedList = new HashMap<>();

  // returns transaction id == logical start timestamp
  public static int begin_transaction() {
    runningTransactions.add(++max_xact);
    ArrayList<ArrayList<Integer>> newWrittenAndInserted = new ArrayList<>();
    newWrittenAndInserted.add(new ArrayList<>());
    newWrittenAndInserted.add(new ArrayList<>());
    dependencies.put(max_xact, new ArrayList<>());
    writtenAndInsertedList.put(max_xact,newWrittenAndInserted);

    return max_xact;
  }

  // create and initialize new object in transaction xact
  public static void insert(int xact, int key, int value) throws Exception
  {
    System.out.println(xact + " insert "+value + " in " + key);
    if (runningTransactions.indexOf(xact) == -1)
      throw new Exception("Transaction not running: insert impossible");
    if (store.containsKey(key)){
      rollback(xact);
      throw new Exception("Trying to insert a value with an already existing key");
    }
    DatabaseObject newObject = new DatabaseObject(value,xact);
    store.put(key,newObject);
    writtenAndInsertedList.get(xact).get(0).add(key);
  }

  // return value of object key in transaction xact
  public static int read(int xact, int key) throws Exception
  {
    System.out.println(xact + " read "+key);
    if (runningTransactions.indexOf(xact) == -1)
      throw new Exception("Transaction not running: read impossible");
    if (!store.containsKey(key)){
      rollback(xact);
      throw new Exception("Trying to read a value that does not exist");
    }
    DatabaseObject object = store.get(key);
    DatabaseVersion mostRecentVersion = object.getMostRecentVersion(xact);
    if (mostRecentVersion.getReadTimeStamp() < xact)
      mostRecentVersion.setReadTimeStamp(xact);
    if (dependencies.get(xact).indexOf(mostRecentVersion.getWriteTimeStamp()) == -1 && mostRecentVersion.getWriteTimeStamp() != xact)
      dependencies.get(xact).add(mostRecentVersion.getWriteTimeStamp());
    return mostRecentVersion.getValue();
  }

  // write value of existing object identified by key in transaction xact
  public static void write(int xact, int key, int value) throws Exception
  {
    System.out.println(xact + " write "+value + " in " + key);
    if (runningTransactions.indexOf(xact) == -1)
      throw new Exception("Transaction not running: write impossible");
    if (!store.containsKey(key)){
      rollback(xact);
      throw new Exception("Trying to write a value with a key that does not exist");
    }
    DatabaseObject object = store.get(key);
    DatabaseVersion version = object.getMostRecentVersion(xact);
    if (xact < version.getReadTimeStamp()){
      rollback(xact);
      throw new Exception("Read time stamp is bigger than our timestamp");
    }
    if (xact == version.getWriteTimeStamp())
      version.setValue(value);
    else{
      if (object.creatorXact != xact)
        dependencies.get(xact).add(object.creatorXact);
      DatabaseVersion newVersion = new DatabaseVersion(value,xact,xact);
      object.versions.add(newVersion);
    }
    writtenAndInsertedList.get(xact).get(1).add(key);
  }

  public static void commit(int xact)   throws Exception {
    System.out.println("commit " + xact);
    if (runningTransactions.indexOf(xact) == -1)
      throw new Exception("Transaction not running: commit impossible");
    if (!dependencies.get(xact).isEmpty() && toCommit.indexOf(xact) == -1)
      toCommit.add(xact);
    else{
      runningTransactions.remove(runningTransactions.indexOf(xact));
      dependencies.remove(xact);
      if (toCommit.indexOf(xact) != -1)
        toCommit.remove(toCommit.indexOf(xact));
      for (ArrayList<Integer> integers : dependencies.values()) {
        if (integers.indexOf(xact) != -1)
          integers.remove(integers.indexOf(xact));
      }
      for (int i = 0; i < toCommit.size(); i++) {
        commit(toCommit.get(i));
      }
    }
  }

  public static void rollback(int xact) throws Exception {
    System.out.println("rollback " + xact);
    if (runningTransactions.indexOf(xact) == -1)
      throw new Exception("Transaction not running: rollback impossible");
    if (toRollback.indexOf(xact) != -1)
      toRollback.remove(toRollback.indexOf(xact));
    for (Map.Entry<Integer, ArrayList<Integer>> integerArrayListEntry : dependencies.entrySet()) {
      if (integerArrayListEntry.getValue().indexOf(xact) != -1 && toRollback.indexOf(integerArrayListEntry.getKey()) == -1)
        toRollback.add(integerArrayListEntry.getKey());
    }
    ArrayList<Integer> hasInserted = writtenAndInsertedList.get(xact).get(0);
    ArrayList<Integer> hasWritten = writtenAndInsertedList.get(xact).get(1);
    for (int i = 0; i < hasInserted.size(); i++) {
      if (store.containsKey(hasInserted.get(i)))
        store.remove(hasInserted.get(i));
    }
    for (int i = 0; i < hasWritten.size(); i++) {
      if (store.containsKey(hasWritten.get(i)))
        store.get(hasWritten.get(i)).deleteVersionWrittenByXact(xact);
    }
    runningTransactions.remove(runningTransactions.indexOf(xact));
    for (int i = 0; i < toRollback.size(); i++) {
      rollback(toRollback.get(i));
    }
  }
}

class DatabaseObject {
  public int creatorXact;
  public ArrayList<DatabaseVersion> versions;

  public DatabaseObject(Integer value, Integer xact){
    this.versions = new ArrayList<>();
    this.creatorXact = xact;
    versions.add(new DatabaseVersion(value, xact, xact));
  }

  public DatabaseVersion getMostRecentVersion(Integer xact){
    DatabaseVersion mostRecentVersion = new DatabaseVersion(0,0,0);
    for (DatabaseVersion version : versions) {
      if (version.getWriteTimeStamp() <= xact && version.getWriteTimeStamp() > mostRecentVersion.getWriteTimeStamp())
        mostRecentVersion = version;
    }
    return mostRecentVersion;
  }

  public void deleteVersionWrittenByXact(Integer xact){
    for (int i = 0; i < versions.size(); i++) {
      if (versions.get(i).getWriteTimeStamp() == xact){
        versions.remove(i);
      }
    }
  }
}
class DatabaseVersion {
  private Integer value;
  private Integer writeTimeStamp;
  private Integer readTimeStamp;

  public DatabaseVersion(Integer value, Integer writeTimeStamp, Integer readTimeStamp) {
    this.value = value;
    this.writeTimeStamp = writeTimeStamp;
    this.readTimeStamp = readTimeStamp;
  }

  public Integer getValue() {
    return value;
  }

  public void setValue(Integer value) {
    this.value = value;
  }

  public Integer getWriteTimeStamp() {
    return writeTimeStamp;
  }

  public void setWriteTimeStamp(Integer writeTimeStamp) {
    this.writeTimeStamp = writeTimeStamp;
  }

  public Integer getReadTimeStamp() {
    return readTimeStamp;
  }

  public void setReadTimeStamp(Integer readTimeStamp) {
    this.readTimeStamp = readTimeStamp;
  }
}
