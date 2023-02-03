## CPEN 431 2022W2 Assignment 4

Name: Megan Ma

Student number: 42442146

### Usage
To run the compiled jar file located in the root directory, run the following command:

`java -Xmx64m -jar A4.jar`

The server will start on port 12345.

### Design choices

- The cache is dynamically resized to optimize performance and minimize the memory footprint.
  The cache is configured to store 500 items initially but will double in size everytime 2/3 of the capacity is reached
  or return to the initial capacity if less than 500 * 2/3 items are stored.
- The server is multithreaded using a thread pool with 5 threads. A scheduled thread will also run every 5 seconds to
  clean up expired items in cache.