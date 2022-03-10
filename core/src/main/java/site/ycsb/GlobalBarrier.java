package site.ycsb;

import mpi.MPI;
import mpi.MPIException;

public class GlobalBarrier {
  public static void barrier() {
    try {
      MPI.COMM_WORLD.barrier();
    } catch (MPIException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
