package com.example.amn;

import java.util.Map;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBufferManager;

public class BoundedRocksDb implements RocksDBConfigSetter {
  static { RocksDB.loadLibrary(); }
  private static final Cache CACHE = new LRUCache(32L << 20, -1, false, 0.1);
  private static final WriteBufferManager WBM = new WriteBufferManager(8L << 20, CACHE);

  @Override
  public void setConfig(String store, Options options, Map<String, Object> configs) {
    BlockBasedTableConfig table = (BlockBasedTableConfig) options.tableFormatConfig();
    table.setBlockCache(CACHE);
    table.setCacheIndexAndFilterBlocks(true);
    table.setCacheIndexAndFilterBlocksWithHighPriority(true);
    table.setPinTopLevelIndexAndFilter(true);
    options.setTableFormatConfig(table);
    options.setWriteBufferManager(WBM);
    options.setMaxWriteBufferNumber(2);
    options.setWriteBufferSize(4L << 20);
  }

  @Override public void close(String store, Options options) {} // shared
}
