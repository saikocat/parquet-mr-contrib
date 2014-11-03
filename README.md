# Parquet Contrib

Parquet Contrib contains stuffs I build when I can't find any implementation out there for Parquet format.

## Features

* Apache Pig MultiStorage-esque

## Apache Pig

### MultiStorage-esque
A Storer (ParquetMultiStorer) that mimics the functionality of [PiggyBank MultiStorage](http://pig.apache.org/docs/r0.12.1/api/org/apache/pig/piggybank/storage/MultiStorage.html) are provided for storing data into Parquet in Pig in multi output format.

#### Build

Apologize for not having it in MavenRepository right now. Please be gentle, this is the first time I use mvn to package (have always been a Gradle guy)

To build the jars:
```
mvn package
```

#### Usage and Example

```
REGISTER '/path/to/parquet-pig-bundle-1.5.0.jar';
REGISTER '/path/to/parquet-hadoop-contrib-1.0.0.jar'; -- generated from the above build step
REGISTER '/path/to/parquet-pig-contrib-1.0.0.jar';    -- generated from the above build step

SET parquet.compression snappy

data = LOAD 'raw' USING PigStorage() as (bucket, name, value);
STORE data INTO '/path/to/output' USING parquet.pig.ParquetMultiStorer('0');
```

**(Required) '/path/to/output'** the path where output directories and files will be created

**(Required) '0'**: Index of the field whose values should be used to create directories and parquet files (field 'bucket' in this case)

Let **'gold'** and **'silver'** be the unique values of field 'bucket'. Then output will look like this:

```
/path/to/output/gold.snappy.parquet/gold-00000.snappy.parquet
/path/to/output/gold.snappy.parquet/gold-00001.snappy.parquet
/path/to/output/gold.snappy.parquet/gold-00002.snappy.parquet
...
/path/to/output/silver.snappy.parquet/silver-00000.snappy.parquet
/path/to/output/silver.snappy.parquet/silver-00001.snappy.parquet
```

The prefix **'0000\*'** is the task-id of the mapper/reducer task executing this store.

#### Caveats
* Internally ParquetMultiStorer creates a Writer for each key, so if you have a large number of buckets (number of unique keys), you winds up creating way too many filehandles all at the same time. It will be very slow or most of the time, you will encounter Out of Memory.
* This method is error-prone against the task failures: partial files are left at the output location.
* Missing unit test gives poor confidence (I do have a few rudimentary ones in Pig and Bash, but doing them in Java is a pain though).
* Astute reader might notice that the contrib package has the same location as the official package (parquet.hadoop), this is because it uses *CodecFactory.BytesCompressor* and *InternalParquetRecordWriter* which are private. Probably a better way will be [Create your own objects](https://github.com/apache/incubator-parquet-mr#create-your-own-objects).
* If you are MultiStorage user, the prefix/task-id might look differently depending on your locale. MultiStorage uses *NumberFormat* to format the task-id (which is influenced by system locale), while I use *String.format* with zero-padded instead to remove this inconsistency.

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0