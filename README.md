## Compiling from source java code 

To compile the source code:
1. Run the following commands: `mvn clean compile` and `mvn package`. This will create a jar file in the target folder by compiling the code.

Note: the source code in either of the first two directories are the same (just version control issue).

## Running the application

To run the application:
1. Run `scp -i cloud_group_rsa /path_to_your_.jar_file ssh_server:/path_to_your_destination_of_copy`. This will copy your jar file onto the server you SSH on
2. Create deployment.yaml file 
3. `kubectl cp <path_to_jar_file>  <pod_name:shared_volume>`
4. Make sure your spark command points to the jar file in the shared volume
5. Run the script using: `time ./script.sh k8s://https://128.232.80.18:6443 cc-group8 spark-cc-group8 andylamp/spark:v3.5.4-amd64 nfs-cc-group8 /test-data 2 -i /test-data/data_100MB.txt`. Change the test-data/data_file_path and number of executors (e.g. here it is 2) as needed.
6. If you would like to run the experiment just run the file `./run-and-plot.sh`, this will put all the measurements in `experiments/<timestamp>/measurements` and `experiments/<timestamp>/graphs`. You can also just run `./run-experiments.sh` if you don't require plots.

## Accessing the virtual volume
If you wish to access the virtual volume just run: `kubectl exec -it group-8-ubuntu-volume -- /bin/bash`

## Running the experiment and plotting scripts

To run the experiment and plotting scripts, just run the command `/.run-and-plot.sh`.

