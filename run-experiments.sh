#!/bin/bash

# Create timestamped directory structure
TIMESTAMP=$1
if [ -z "$TIMESTAMP" ]; then
  TIMESTAMP=$(date +"%Y%m%dT%H%M%S")
fi

MEASUREMENT_DIR="experiments/${TIMESTAMP}/measurements"
mkdir -p "${MEASUREMENT_DIR}"

START_TIMES_FILE="experiments/${TIMESTAMP}/start_times.csv"
echo "File,Executors,Run,Start_Time" >"$START_TIMES_FILE"

# CONFIGURATION
EXECUTORS=(2 4 6)
FILES=("data-100MB" "data-200MB" "data-500MB")
REPEATS=3

for file_prefix in "${FILES[@]}"; do
  input_file="${file_prefix}.txt"

  # Add .csv extension
  csv_name="${file_prefix}.csv"
  csv_path="${MEASUREMENT_DIR}/${csv_name}"

  # Write the required header to the new CSV
  echo "Workers, Execution Time 1, Execution Time 2, Execution Time 3" >"${csv_path}"

  # Iterate executor counts
  for execs in "${EXECUTORS[@]}"; do
    # Array to hold the 3 times
    times=()

    # Loop 3: Run the 3 repetitions
    for ((i = 1; i <= REPEATS; i++)); do
      echo "Running: Execs=$execs, File=$input_file, Run=$i"

      # Capture time taken
      TIME_TAKEN=$({ /usr/bin/time -f '%e' sh -c "./script.sh \"\" \"\" \"\" \"\" \"\" \"\" \"$execs\" -i \"/test-data/$input_file\" >/dev/null 2>/dev/null"; } 2>&1)
      echo "Time taken: $TIME_TAKEN"

      # Store the time in array
      times+=("$TIME_TAKEN")

      # Save the raw outputs from each run to a local backup
      LOCAL_BACKUP="local_experiment_results/exec${execs}_${input_file}_run${i}"
      mkdir -p "$LOCAL_BACKUP"
      echo "Backing up results to $LOCAL_BACKUP..."
      kubectl cp group-8-ubuntu-volume:/test-data/CloudComputingCoursework_Group8/ "$LOCAL_BACKUP/"

      # Clean the output file on persistent volume to avoid eror
      kubectl exec group-8-ubuntu-volume -- rm -rf /test-data/CloudComputingCoursework_Group8

      sleep 5
    done

    # Write the row (e.g., "2, 0m39.962s, 0m37.912s, 0m39.938s") to the CSV
    echo "$execs, ${times[0]}, ${times[1]}, ${times[2]}" >>"${csv_path}"
  done
done

echo "All experiments complete. Times are saved to $RESULTS_FILE and CSV files are saved to local_experiment_results"
