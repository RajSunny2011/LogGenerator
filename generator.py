import csv
import json
import sys
import time

def stream_logs_from_csv(csv_path, log_file_path):
    """
    Reads a CSV file, converts each row to JSON, and writes it to a log file.
    The process loops indefinitely to simulate a continuous stream of logs.
    """
    if len(sys.argv) < 3:
        print("Usage: python log_generator.py <csv_file_path> <log_file_path>")
        sys.exit(1)

    print(f"Starting log stream from {csv_path} to {log_file_path}...")
    
    while True:
        try:
            with open(csv_path, mode='r') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    # Append the JSON string to the target log file
                    with open(log_file_path, "a") as log_file:
                        log_file.write(json.dumps(row) + '\n')
                    
                    # Wait for a short interval to simulate real-time stream
                    time.sleep(1) 
        except FileNotFoundError:
            print(f"Error: CSV file not found at {csv_path}. Retrying in 10 seconds.")
            time.sleep(10)
        except Exception as e:
            print(f"An error occurred: {e}. Restarting the loop in 10 seconds.")
            time.sleep(10)

if __name__ == "__main__":
    csv_file_path = sys.argv[1]
    log_file_path = sys.argv[2]
    stream_logs_from_csv(csv_file_path, log_file_path)