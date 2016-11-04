package suadb.server;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import suadb.remote.RemoteDriver;
import suadb.remote.RemoteDriverImpl;

public class Startup {
   public static void main(String args[]) throws Exception {
      // configure and initialize the database
      SuaDB.init(args[0]);
      
      // create a registry specific for the server on the default port
      Registry reg = LocateRegistry.createRegistry(1099);
      
      // and post the server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database server ready");
   }
}
