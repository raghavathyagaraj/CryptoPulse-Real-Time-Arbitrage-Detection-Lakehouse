import subprocess
import time
import os
import signal
import sys

# Define the processes we want to run
processes = {
    "producer": "src/producers/main.py",
    "bronze": "src/jobs/ingestion_bronze.py",
    "silver": "src/jobs/process_silver.py",
    "gold":   "src/jobs/gold_layer.py"
}

running_procs = []

def stop_all(sig, frame):
    """Gracefully kill all subprocesses on Ctrl+C"""
    print("\n Stopping all services...")
    for p in running_procs:
        p.terminate()
    sys.exit(0)

# Register the Ctrl+C signal handler
signal.signal(signal.SIGINT, stop_all)

def main():
    # Create a logs directory
    os.makedirs("logs", exist_ok=True)

    print(" Starting Crypto Lakehouse Pipeline...")
    print(f"📂 Logs will be written to the 'logs/' directory.")
    print("---------------------------------------------------")

    for name, script_path in processes.items():
        print(f"Starting {name.upper()} layer...")
        
        # Open log files
        stdout_log = open(f"logs/{name}.out", "w")
        stderr_log = open(f"logs/{name}.err", "w")

        # Launch the process using 'uv run'
        # This ensures we use the correct virtual environment
        proc = subprocess.Popen(
            ["uv", "run", script_path],
            stdout=stdout_log,
            stderr=stderr_log
        )
        
        running_procs.append(proc)
        time.sleep(2) # Give it a moment to initialize

    print("---------------------------------------------------")
    print("  All systems GO! The pipeline is running in the background.")
    print("   Tail the logs to see activity (e.g., 'tail -f logs/producer.out')")
    print("   Press Ctrl+C to stop everything.")
    
    # Keep the main script alive to listen for Ctrl+C
    signal.pause()

if __name__ == "__main__":
    main()