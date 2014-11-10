### Version 1.1.0 ### 
* Use Guava LoadingCache to control the maximum number of open file handles,
  thus avoids the Heap error.
* Add 'maxNumberOfWriters' parameter to dictate how many open file handles can
  be created.
* Pig/ParquetMultiStorer now accepts a 2nd parameter 'maxNumberOfWriters'
  (default to '10')

### Version 1.0.0 ### 
