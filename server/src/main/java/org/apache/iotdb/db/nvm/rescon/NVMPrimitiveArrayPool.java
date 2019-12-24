package org.apache.iotdb.db.nvm.rescon;

import java.util.ArrayDeque;
import java.util.EnumMap;
import org.apache.iotdb.db.nvm.PerfMonitor;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager.NVMSpace;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class NVMPrimitiveArrayPool {

  /**
   * data type -> Array<PrimitiveArray>
   */
  private static final EnumMap<TSDataType, ArrayDeque<NVMSpace>> primitiveArraysMap = new EnumMap<>(TSDataType.class);

  public static final int ARRAY_SIZE = 128;

  static {
    primitiveArraysMap.put(TSDataType.BOOLEAN, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.INT32, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.INT64, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.FLOAT, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.DOUBLE, new ArrayDeque());
    primitiveArraysMap.put(TSDataType.TEXT, new ArrayDeque());
  }

  public static NVMPrimitiveArrayPool getInstance() {
    return INSTANCE;
  }

  private static final NVMPrimitiveArrayPool INSTANCE = new NVMPrimitiveArrayPool();


  private NVMPrimitiveArrayPool() {}

  public synchronized NVMSpace getPrimitiveDataListByType(TSDataType dataType) {
    long time = System.currentTimeMillis();
    ArrayDeque<NVMSpace> dataListQueue = primitiveArraysMap.computeIfAbsent(dataType, k ->new ArrayDeque<>());
    NVMSpace nvmSpace = dataListQueue.poll();

    long size = NVMSpaceManager.getPrimitiveTypeByteSize(dataType);
    if (nvmSpace == null) {
      nvmSpace = NVMSpaceManager.getInstance().allocate(size * ARRAY_SIZE, dataType);
    }

    PerfMonitor.add("NVM.getDataList", System.currentTimeMillis() - time);
    return nvmSpace;
  }


  public synchronized void release(NVMSpace nvmSpace, TSDataType dataType) {
    // TODO freeslotmap?

    primitiveArraysMap.get(dataType).add(nvmSpace);
  }

  /**
   * @param size needed capacity
   * @return an array of primitive data arrays
   */
  public synchronized NVMSpace[] getDataListsByType(TSDataType dataType, int size) {
    int arrayNumber = (int) Math.ceil((float) size / (float) ARRAY_SIZE);
    NVMSpace[] nvmSpaces = new NVMSpace[arrayNumber];
    for (int i = 0; i < arrayNumber; i++) {
      nvmSpaces[i] = getPrimitiveDataListByType(dataType);
    }
    return nvmSpaces;
  }
}
