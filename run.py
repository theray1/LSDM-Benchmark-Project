import subprocess
import time

# Function to run a command and measure execution time
def run_command(command):
    subprocess.Popen(command, shell=True).wait()


def run_command_profiled(command):
    start_time = time.time()
    process = subprocess.Popen(command, shell=True)
    process.wait()
    end_time = time.time()
    return end_time - start_time

# Define your cluster and job configurations
num_workers = 2  # Replace with the desired number of workers
project_id = "lsdm-project-401907"
bucket_name = "gs://kick_the/"
mode = "pig"
region = "europe-west4"

# Create the cluster
create_cluster_command = (
    f"gcloud dataproc clusters create cluster-a35a "
    f"--enable-component-gateway "
    f"--region {region} "
    f"--master-machine-type n1-standard-4 "
    f"--master-boot-disk-size 500 "
    f"--num-workers {num_workers} "
    f"--worker-machine-type n1-standard-4 "
    f"--worker-boot-disk-size 500 "
    f"--image-version 2.0-debian10 "
    f"--project {project_id}"
)
run_command(create_cluster_command)

# Copy data to the cluster
copy_data_command = f"gsutil cp gs://public_lddm_data/small_page_links.nt gs://{bucket_name}/"
run_command(copy_data_command)

# Copy Pig code to the cluster
copy_pig_code_command = "gsutil cp pigpagerank.py gs://kick_the/"
run_command(copy_pig_code_command)

# Clean out directory
clean_directory_command = "gsutil rm -rf gs://kick_the/out"
run_command(clean_directory_command)

# Run the Pig job
run_pig_job_command = (
    f"gcloud dataproc jobs submit pig "
    f"--region {region} "
    f"--cluster cluster-a35a "
    f"-f gs://kick_the/dataproc.py"
)
run_pig_job_time = run_command_profiled(run_pig_job_command)

# Access results
access_results_command = "gsutil cat gs://kick_the/out/pagerank_data_10/part-r-00000"
run_command(access_results_command)

# Delete the cluster
delete_cluster_command = (
    f"gcloud dataproc clusters stop cluster-a35a "
    f"--region {region} "
    f"--quiet"
)
run_command(delete_cluster_command)

print("Run Pig Job Time: ", run_pig_job_time)

with open("res.txt", "a+") as myfile:
    myfile.write(f"Run Pig Job Time: {run_pig_job_time} workers: {num_workers} mode: {mode}")