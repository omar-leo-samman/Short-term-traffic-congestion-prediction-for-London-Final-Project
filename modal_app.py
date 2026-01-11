import modal
import subprocess

# 1. Define the Image
# We install dependencies from requirements.txt and add jupyter tools
image = (
    modal.Image.debian_slim()
    .pip_install_from_requirements("requirements.txt")
    .pip_install("jupyter", "nbconvert", "ipykernel")
)

app = modal.App("london-traffic-scheduler", image=image)

# 2. Define Secrets
# You must create these in the Modal Dashboard (modal.com/secrets)
secrets = [
    modal.Secret.from_name("hopsworks-api-key"),
    modal.Secret.from_name("huggingface-api-key"),
    modal.Secret.from_name("tomtom-api-key"),
    modal.Secret.from_name("tfl-app-key"),
]

# 3. Mount the notebooks directory so the container can see them
notebooks_mount = modal.Mount.from_local_dir("notebooks", remote_path="/root/notebooks")

# 4. Define the Scheduled Function
@app.function(
    secrets=secrets,
    mounts=[notebooks_mount],
    schedule=modal.Cron("*/30 * * * *"),  # Runs every 30 minutes
    timeout=1800  # Allow up to 30 minutes execution time
)
def run_traffic_pipeline():
    notebooks_to_run = [
        "/root/notebooks/07_realtime_feature_update_modal_30min.ipynb",
        "/root/notebooks/08_inference_pipeline.ipynb"
    ]
    
    for nb_path in notebooks_to_run:
        print(f"🚀 Starting execution of: {nb_path}")
        
        # We use nbconvert to execute the notebook in place
        try:
            subprocess.check_call(
                [
                    "jupyter",
                    "nbconvert",
                    "--to", "notebook",
                    "--execute",
                    "--inplace",
                    nb_path
                ]
            )
            print(f"✅ Successfully finished: {nb_path}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error executing {nb_path}")
            raise e