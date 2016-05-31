/*
 * Copyright 2014 FullContact, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fullcontact.sstable.index;

import com.fullcontact.sstable.hadoop.IndexOffsetScanner;
import com.fullcontact.sstable.hadoop.mapreduce.HadoopSSTableConstants;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import gnu.trove.list.array.TLongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

/**
 * This is basically building an index of chunks into the Cassandra Index.db file--an index index. The final index
 * created here defines the splits that will be used downstream. Split size is configurable and defaults to 1024 MB.
 *
 * 索引的索引. 第一个索引是SSTable本身数据文件的索引. 第二个索引是对索引文件再建索引. 相当于data file's metadata's metadata
 * 索引文件可以看做是一个文件, 要对文件建立索引. 正如原先的索引文件是对数据文件建立的索引.
 */
public class SSTableIndexIndex {

    public static final String SSTABLE_INDEX_SUFFIX = ".Index";
    public static final String SSTABLE_INDEX_IN_PROGRESS_SUFFIX = ".Index.inprogress";

    private final List<Chunk> chunks = Lists.newArrayList();

    /**
     * Read an existing index. Reads and returns the index index, which is a list of chunks
     * defined by the Cassandra Index.db file along with the configured split size.
     *
     * Cassandra的Index.db本身就是对数据文件建立的索引. 索引是为了能够快速定位到指定的PK所在的位置.
     * 和DBMS中的索引一样,都是为了能够更快地获取到指定行的数据, 即能够根据索引字段更快地查询到需要的数据.
     *
     * @param fileSystem Hadoop file system.
     * @param sstablePath SSTable Index.db.
     * @return Index of chunks.
     * @throws IOException
     */
    public static SSTableIndexIndex readIndex(final FileSystem fileSystem, final Path sstablePath) throws IOException {
        final Closer closer = Closer.create();
        //假设sstablePath是AAA-Index.db, 则indexPath=AAA-Index.db.Index
        final Path indexPath = sstablePath.suffix(SSTABLE_INDEX_SUFFIX);

        // Detonate if we don't have an index. 所以indexPath必须事先生成! 否则就没办法读取里面的数据了!
        final FSDataInputStream inputStream = closer.register(fileSystem.open(indexPath));

        final SSTableIndexIndex indexIndex = new SSTableIndexIndex();
        try {
            while (inputStream.available() != 0) {
                //idnexPath文件的每一行只有两个字段, 表示每一个chunk的start和end的偏移量
                indexIndex.add(inputStream.readLong(), inputStream.readLong());
            }
        } finally {
            closer.close();
        }

        return indexIndex;
    }

    /**
     * Add a chunk with a start and end offset.
     *
     * @param start Beginning of the chunk.
     * @param end End of the chunk.
     */
    private void add(final long start, final long end) {
        this.chunks.add(new Chunk(start, end));
    }

    /**
     * Create and write an index index based on the input Cassandra Index.db file. 根据Index.db生成Index.db.Index文件
     * Read the Index.db and generate chunks (splits) based on the configured chunk size.
     *
     * @param fileSystem Hadoop file system.
     * @param sstablePath SSTable Index.db.
     * @throws IOException
     */
    public static void writeIndex(final FileSystem fileSystem, final Path sstablePath) throws IOException {

        final Configuration configuration = fileSystem.getConf();

        //SSTable默认以1G分隔. 为什么是1G,因为MR处理时,是以数据块的大小决定任务数的.数据块默认512M,可以设置为1G.
        final long splitSize = configuration.getLong(HadoopSSTableConstants.HADOOP_SSTABLE_SPLIT_MB,
                HadoopSSTableConstants.DEFAULT_SPLIT_MB) * 1024 * 1024;

        final Closer closer = Closer.create();

        //要生成的文件名称=索引文件+Index后缀
        final Path outputPath = sstablePath.suffix(SSTABLE_INDEX_SUFFIX);
        final Path inProgressOutputPath = sstablePath.suffix(SSTABLE_INDEX_IN_PROGRESS_SUFFIX);

        boolean success = false;
        try {
            //创建输出文件的输出流对象, 准备往输出流中写数据
            final FSDataOutputStream os = closer.register(fileSystem.create(inProgressOutputPath));

            final TLongArrayList splitOffsets = new TLongArrayList();
            long currentStart = 0;
            long currentEnd = 0;
            long currentDataStart = 0;
            long currentDataEnd = 0;
            final IndexOffsetScanner index = closer.register(new IndexOffsetScanner(sstablePath, fileSystem));

            //TODO 这里又两个while循环是怎么回事?
            while (index.hasNext()) {
                // NOTE: This does not give an exact size of this split in bytes but a rough estimate.
                // This should be good enough since it's only used for sorting splits by size in hadoop land.
                while (currentDataEnd - currentDataStart < splitSize && index.hasNext()) {
                    final IndexOffsetScanner.IndexEntry indexEntry = index.next();
                    currentEnd = indexEntry.getIdxOffset();
                    currentDataEnd = indexEntry.getDataOffset();
                    splitOffsets.add(currentEnd);
                }

                // Record the split
                final long[] offsets = splitOffsets.toArray();
                os.writeLong(offsets[0]); // Start
                os.writeLong(offsets[offsets.length - 1]); // End

                // Clear the offsets
                splitOffsets.clear();

                if (index.hasNext()) {
                    final IndexOffsetScanner.IndexEntry indexEntry = index.next();
                    currentStart = indexEntry.getIdxOffset();
                    currentDataStart = indexEntry.getDataOffset();
                    currentEnd = currentStart;
                    currentDataEnd = currentDataStart;
                    splitOffsets.add(currentStart);
                }
            }

            success = true;
        } finally {
            closer.close();

            if (!success) {
                fileSystem.delete(inProgressOutputPath, false);
            } else {
                fileSystem.rename(inProgressOutputPath, outputPath);
            }
        }
    }

    /**
     * Return the chunks (splits) defined by this index.
     *
     * @return Chunks.
     */
    public List<Chunk> getOffsets() {
        return Lists.newArrayList(chunks);
    }

    /**
     * Simple chunk which contains a start and end.
     */
    public class Chunk {
        private final long start;
        private final long end;

        public Chunk(final long start, final long end) {
            this.start = start;
            this.end = end;
        }

        public long getEnd() {
            return end;
        }

        public long getStart() {
            return start;
        }

        @Override
        public String toString() {
            return "Chunk{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }
    }
}