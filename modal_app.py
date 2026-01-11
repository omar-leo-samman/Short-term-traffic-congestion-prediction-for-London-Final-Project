import modal
import subprocess
import os
import sys

# --- 1. CONFIGURACIÓN DE RUTAS LOCALES ---
base_path = os.path.dirname(__file__)
local_notebooks_path = os.path.join(base_path, "notebooks")
local_src_path = os.path.join(base_path, "src")

if not os.path.exists(local_notebooks_path):
    raise RuntimeError(f"❌ ERROR: No encuentro la carpeta 'notebooks' en: {local_notebooks_path}")

# --- 2. DEFINICIÓN DE LA IMAGEN ---
image = (
    modal.Image.debian_slim()
    .pip_install_from_requirements("requirements.txt")
    # AQUI AGREGAMOS "confluent-kafka" 👇
    .pip_install("jupyter", "nbconvert", "ipykernel", "pyarrow", "confluent-kafka")
    .add_local_dir(local_notebooks_path, remote_path="/root/notebooks")
    .add_local_dir(local_src_path, remote_path="/root/src")
)

app = modal.App("london-traffic-scheduler", image=image)

# --- 3. SECRETOS ---
secrets = [
    modal.Secret.from_name("HOPSWORKS_API_KEY"),
    modal.Secret.from_name("HUGGINGFACE_API_KEY"),
    modal.Secret.from_name("TOMTOM_API_KEY"),
    modal.Secret.from_name("TFL_APP_KEY"),
]

# --- 4. FUNCIÓN PROGRAMADA ---
@app.function(
    secrets=secrets,
    schedule=modal.Cron("*/30 * * * *"),
    timeout=1800 
)
def run_traffic_pipeline():
    notebooks_to_run = [
        "/root/notebooks/07_realtime_feature_update_modal_30min.ipynb",
        "/root/notebooks/08_inference_pipeline.ipynb"
    ]
    
    env_vars = os.environ.copy()
    env_vars["PYTHONPATH"] = "/root" 

    print("🚀 Iniciando pipeline de tráfico de Londres...")

    for nb_path in notebooks_to_run:
        print(f"▶️  Ejecutando: {nb_path} ...")
        
        result = subprocess.run(
            [
                "jupyter",
                "nbconvert",
                "--to", "notebook",
                "--execute",
                "--inplace",
                "--ExecutePreprocessor.kernel_name=python3",
                nb_path
            ],
            capture_output=True,
            text=True,
            env=env_vars
        )
        
        if result.returncode != 0:
            print(f"\n❌ FALLÓ EL NOTEBOOK: {nb_path}")
            print("================= LOG DEL ERROR (STDOUT/STDERR) =================")
            if result.stdout:
                print("--- SALIDA ESTÁNDAR ---")
                print(result.stdout)
            if result.stderr:
                print("--- ERRORES ---")
                print(result.stderr)
            print("=================================================================")
            raise Exception(f"La ejecución se detuvo porque falló {nb_path}")
        else:
            print(f"✅ Terminado con éxito: {nb_path}")

    print("🏁 Pipeline completado correctamente.")