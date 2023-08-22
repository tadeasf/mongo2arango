import os
import subprocess

# Directory containing the population scripts
scripts_dir = "/home/taddy.fort/mongo2arango/mongo2arango/populationScripts"

# Get the list of all Python scripts in the directory
scripts = [
    os.path.join(scripts_dir, f) for f in os.listdir(scripts_dir) if f.endswith(".py")
]

# Total number of scripts to run
total_scripts = len(scripts)

# Progress tracking
progress = 0

print("Starting population of the database...")

# Run each script one by one
for script in scripts:
    print(f"Running {script}... ({progress}/{total_scripts})")

    # Run the script using a subprocess and capture the output and errors
    result = subprocess.run(
        ["python", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Check if the script ran successfully
    if result.returncode == 0:
        print(f"Successfully populated data from {script}")
    else:
        # Print error message and continue with the next script
        print(
            f"An error occurred while running {script}: {result.stderr.decode('utf-8')}"
        )

    # Update progress
    progress += 1

print(f"Population completed. {progress}/{total_scripts} scripts executed.")
