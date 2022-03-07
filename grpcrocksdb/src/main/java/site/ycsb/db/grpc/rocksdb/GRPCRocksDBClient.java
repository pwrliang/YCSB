package site.ycsb.db.grpc.rocksdb;


import mpi.MPI;
import mpi.MPIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * GRPCRocksDBClient
 */
public class GRPCRocksDBClient extends DB {
  static final String PROPERTY_ADDR = "grpc.addr";
  private static final Logger LOGGER = LoggerFactory.getLogger(GRPCRocksDBClient.class);

  public static class KVPairs {
    public List<byte[]> keys;
    public List<byte[]> values;
  }

  static {
    System.loadLibrary("grpcrocksdbjni");    // loads libhello.so
  }

  private static long handle;

  private static long references;

  public native long connect(String addr);

  public native byte[] get(long handle, final byte[] key);

  public native int put(long handle, final byte[] key, final byte[] value);

  public native int delete(long handle, final byte[] key);

  public native KVPairs scan(long handle, final byte[] startkey, final int recordcount, final boolean return_keys);

  public native void disconnect(long handle);

  private void PrintTime(String msg) {
    SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    System.out.println(msg + " " + sdf3.format(timestamp));
  }

  @Override
  public void init() throws DBException {
    synchronized (GRPCRocksDBClient.class) {
      if (handle == 0) {
        String addr = getProperties().getProperty(PROPERTY_ADDR);
        handle = connect(addr);
      }
      references++;
    }
    try {
      MPI.Init(new String[0]);
      System.out.println("Client number: " + MPI.COMM_WORLD.getSize());
      MPI.COMM_WORLD.barrier();
    } catch (MPIException e) {
      e.printStackTrace();
      System.exit(1);
    }
    PrintTime("Ready Time:");
  }

  @Override
  public site.ycsb.Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    byte[] val = get(handle, key.getBytes(StandardCharsets.UTF_8));
    if (val != null) {
      deserializeValues(val, fields, result);
      return site.ycsb.Status.OK;
    }
    return site.ycsb.Status.NOT_FOUND;
  }

  @Override
  public site.ycsb.Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    KVPairs kvPairs = scan(handle, startkey.getBytes(StandardCharsets.UTF_8), recordcount, false);
    for (byte[] val : kvPairs.values) {
      final HashMap<String, ByteIterator> values = new HashMap<>();
      deserializeValues(val, fields, values);
      result.add(values);
    }
    return site.ycsb.Status.OK;
  }

  @Override
  public site.ycsb.Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      final Map<String, ByteIterator> result = new HashMap<>();
      final byte[] currentValues = get(handle, key.getBytes(StandardCharsets.UTF_8));
      if (currentValues == null) {
        return site.ycsb.Status.NOT_FOUND;
      }
      deserializeValues(currentValues, null, result);

      //update
      result.putAll(values);

      //store
      int err = put(handle, key.getBytes(StandardCharsets.UTF_8), serializeValues(result));
      if (err == 0) {
        return site.ycsb.Status.OK;
      } else {
        return site.ycsb.Status.ERROR;
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      return site.ycsb.Status.ERROR;
    }
  }

  @Override
  public site.ycsb.Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      put(handle, key.getBytes(StandardCharsets.UTF_8), serializeValues(values));
      return site.ycsb.Status.OK;
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      return site.ycsb.Status.ERROR;
    }
  }

  @Override
  public site.ycsb.Status delete(String table, String key) {
    System.out.println("Delete " + key);
    return delete(handle, key.getBytes(StandardCharsets.UTF_8)) == 0 ? site.ycsb.Status.OK : site.ycsb.Status.ERROR;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (GRPCRocksDBClient.class) {
      PrintTime("End Time:");
      if (references == 1) {
        disconnect(handle);
        handle = 0;
        try {
          MPI.Finalize();
        } catch (MPIException e) {
          e.printStackTrace();
          System.exit(1);
        }
      }
      references--;
    }
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields, final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }
}
