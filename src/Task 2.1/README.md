# Follow these step to achieve the requirements

1. Run command
```
./run.sh KMeans /input /output
```
Note: The run.sh is my custom file for quickly start the application.
2. The output will be looked like the following:
```
   hadoop bin/hadoop fs -ls /output
Found 88 items
drwxr-xr-x   - hadoop supergroup          0 2024-05-01 21:17 /output_10
-rw-r--r--   1 hadoop supergroup        990 2024-05-01 21:17 /output_10_clusters-r-00000
drwxr-xr-x   - hadoop supergroup          0 2024-05-01 21:17 /output_11
-rw-r--r--   1 hadoop supergroup        990 2024-05-01 21:17 /output_11_clusters-r-00000
drwxr-xr-x   - hadoop supergroup          0 2024-05-01 21:17 /output_12
-rw-r--r--   1 hadoop supergroup        990 2024-05-01 21:17 /output_12_clusters-r-00000
drwxr-xr-x   - hadoop supergroup          0 2024-05-01 21:17 /output_13
-rw-r--r--   1 hadoop supergroup        990 2024-05-01 21:17 /output_13_clusters-r-00000
drwxr-xr-x   - hadoop supergroup          0 2024-05-01 21:17 /output_14
-rw-r--r--   1 hadoop supergroup        990 2024-05-01 21:17 /output_14_clusters-r-00000
....
```
3. See the last ones which are usually /output_20/part-r-00000 and /output_20_clusters-r-00000. They are the required files.