/**
 * Copyright 2012 Twitter, Inc.
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
package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import parquet.column.ParquetProperties.WriterVersion;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.schema.MessageType;

/**
 * Writes records to a Parquet file
 *
 * @param <K> the key to be split by
 * @param <T> the type of the materialized records
 * @author Julien Le Dem, Nguyen Duc Hoa
 * @see ParquetMultiOutputFormat
 */
public class ParquetMultiRecordWriter<K, T> extends RecordWriter<K, T> {

    private ParquetFileWriter w;
    private Path workPath;
    private String extension;
    private String taskId;
    private Configuration conf;
    private WriteSupport<T> writeSupport;
    private MessageType schema;
    private Map<String, String> extraMetaData;
    private int blockSize;
    private int pageSize;
    private BytesCompressor compressor;
    private int dictionaryPageSize;
    private boolean enableDictionary;
    private boolean validating;
    private WriterVersion writerVersion;

    private Map<String, InternalParquetRecordWriter<T>> storeMap =
            new HashMap<String, InternalParquetRecordWriter<T>>();

    /**
     * @param workPath           the path to the output directory (temporary)
     * @param extension          the codec extension + .parquet
     * @param taskId             the zero-padded task ID
     * @param writeSupport       the class to convert incoming records
     * @param schema             the schema of the records
     * @param extraMetaData      extra meta data to write in the footer of the file
     * @param blockSize          the size of a block in the file (this will be approximate)
     * @param compressor         the compressor used to compress the pages
     * @param dictionaryPageSize the threshold for dictionary size
     * @param enableDictionary   to enable the dictionary
     * @param validating         if schema validation should be turned on
     */
    public ParquetMultiRecordWriter(
            Path workPath,
            String extension,
            String taskId,
            Configuration conf,
            WriteSupport<T> writeSupport,
            MessageType schema,
            Map<String, String> extraMetaData,
            int blockSize, int pageSize,
            BytesCompressor compressor,
            int dictionaryPageSize,
            boolean enableDictionary,
            boolean validating,
            WriterVersion writerVersion) {
        this.workPath = workPath;
        this.extension = extension;
        this.taskId = taskId;
        this.conf = conf;
        this.writeSupport = writeSupport;
        this.schema = schema;
        this.extraMetaData = extraMetaData;
        this.blockSize = blockSize;
        this.pageSize = pageSize;
        this.compressor = compressor = compressor;
        this.dictionaryPageSize = dictionaryPageSize;
        this.enableDictionary = enableDictionary;
        this.validating = validating;
        this.writerVersion = writerVersion;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        for (InternalParquetRecordWriter writer : storeMap.values()) {
            writer.close();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private InternalParquetRecordWriter getStore(String fieldValue) throws IOException {
        InternalParquetRecordWriter store = storeMap.get(fieldValue);
        if (store == null) {
            String extension = this.getExtension();
            Path path = new Path(fieldValue + extension, fieldValue + '-'
                    + this.getTaskId() + extension);
            Path file = new Path(this.getWorkPath(), path);

            ParquetFileWriter fw = new ParquetFileWriter(this.getConf(), this.getSchema(), file);
            fw.start();

            store = new InternalParquetRecordWriter<T>(
                    fw,
                    this.getWriteSupport(),
                    this.getSchema(),
                    this.getExtraMetaData(),
                    this.getBlockSize(),
                    this.getPageSize(),
                    this.getCompressor(),
                    this.getDictionaryPageSize(),
                    this.isEnableDictionary(),
                    this.isValidating(),
                    this.getWriterVersion());
            storeMap.put(fieldValue, store);
        }
        return store;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void write(K key, T value) throws IOException, InterruptedException {
        getStore(key.toString()).write(value);
    }

    public Path getWorkPath() {
        return workPath;
    }

    public Configuration getConf() {
        return conf;
    }

    public String getExtension() {
        return extension;
    }

    public String getTaskId() {
        return taskId;
    }

    public ParquetFileWriter getW() {
        return w;
    }

    public WriteSupport<T> getWriteSupport() {
        return writeSupport;
    }

    public MessageType getSchema() {
        return schema;
    }

    public Map<String, String> getExtraMetaData() {
        return extraMetaData;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public BytesCompressor getCompressor() {
        return compressor;
    }

    public int getDictionaryPageSize() {
        return dictionaryPageSize;
    }

    public boolean isEnableDictionary() {
        return enableDictionary;
    }

    public boolean isValidating() {
        return validating;
    }

    public WriterVersion getWriterVersion() {
        return writerVersion;
    }
}
