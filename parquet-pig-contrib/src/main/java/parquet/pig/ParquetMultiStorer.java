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
package parquet.pig;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.io.ParquetEncodingException;
import parquet.pig.TupleWriteSupport;
import parquet.hadoop.ParquetMultiOutputFormat;

/**
 * A pig storer for the Parquet file format for splitting the output
 * data into multiple directories and files dynamically based on
 * user specified key field in the output tuple.
 *
 * Sample Usage:
 * <code>
 * data = LOAD 'raw' USING PigStorage() as (bucket, name, value);
 * STORE data INTO '/path/to/output' USING parquet.pig.ParquetMultiStorer('0');
 * </code>
 * 
 * (Required) '/path/to/output' the path where output directories and files will be created
 * (Required) '0': Index of the field whose values should be used to create 
 * directories and parquet files (field 'bucket' in this case)
 *
 * Let 'gold' and 'silver' be the unique values of field 'bucket'. Then output will
 * look like this:
 *
 * /path/to/output/gold.snappy.parquet/gold-00000.snappy.parquet
 * /path/to/output/gold.snappy.parquet/gold-00001.snappy.parquet
 * /path/to/output/gold.snappy.parquet/gold-00002.snappy.parquet
 * ...
 * /path/to/output/silver.snappy.parquet/silver-00000.snappy.parquet
 * /path/to/output/silver.snappy.parquet/silver-00001.snappy.parquet
 *
 * The prefix '0000*' is the task-id of the mapper/reducer task executing
 * this store.
 *
 * see {@link ParquetMultiOutputFormat} for available parameters.
 * <p/>
 * It uses a TupleWriteSupport to write Tuples into the ParquetOutputFormat
 * The Pig schema is automatically converted to the Parquet schema using {@link PigSchemaConverter}
 * and stored in the file
 *
 * @author Julien Le Dem, Nguyen Duc Hoa
 */
public class ParquetMultiStorer extends StoreFunc implements StoreMetadata {

    private static final String SCHEMA = "schema";
    private static final int MAX_NO_OF_WRITERS = 10;

    private RecordWriter<String, Tuple> recordWriter;

    private String signature;

    private int splitFieldIndex = 0;
    private int maxNumberOfWriters;

    public ParquetMultiStorer(String splitFieldIndex)
            throws IOException {
        this(splitFieldIndex, Integer.toString(MAX_NO_OF_WRITERS));
    }

    public ParquetMultiStorer(String splitFieldIndex, String maxNumberOfWriters)
            throws IOException {
        this.splitFieldIndex = Integer.parseInt(splitFieldIndex);
        this.maxNumberOfWriters = Integer.parseInt(maxNumberOfWriters);

        if (this.maxNumberOfWriters <= 0)
            throw new IOException("Max Number of Writers must be larger than 0");
    }

    private Properties getProperties() {
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
                udfc.getUDFProperties(this.getClass(), new String[]{signature});
        return p;
    }

    private Schema getSchema() {
        try {
            final String schemaString = getProperties().getProperty(SCHEMA);
            if (schemaString == null) {
                throw new ParquetEncodingException("Can not store relation in Parquet as the schema is unknown");
            }
            return Utils.getSchemaFromString(schemaString);
        } catch (ParserException e) {
            throw new ParquetEncodingException("can not get schema from context", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        super.setStoreFuncUDFContextSignature(signature);
        this.signature = signature;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        getProperties().setProperty(SCHEMA, s.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OutputFormat<String, Tuple> getOutputFormat() throws IOException {
        Schema pigSchema = getSchema();
        return new ParquetMultiOutputFormat<String, Tuple>(new TupleWriteSupport(pigSchema), getMaxNumberOfWriters());
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"rawtypes", "unchecked"}) // that's how the base class is defined
    @Override
    public void prepareToWrite(RecordWriter recordWriter) throws IOException {
        this.recordWriter = recordWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putNext(Tuple tuple) throws IOException {
        int splitFieldIndex = getSplitFieldIndex();

        if (tuple.size() <= splitFieldIndex) {
            throw new IOException("split field index:" + splitFieldIndex
                    + " >= tuple size:" + tuple.size());
        }

        try {
            Object field = tuple.get(splitFieldIndex);
            this.recordWriter.write(String.valueOf(field), tuple);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ParquetEncodingException("Interrupted while writing", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job)
            throws IOException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeStatistics(ResourceStatistics resourceStatistics, String location, Job job)
            throws IOException {
    }

    public int getSplitFieldIndex() {
        return splitFieldIndex;
    }

    public int getMaxNumberOfWriters() {
        return maxNumberOfWriters;
    }
}
