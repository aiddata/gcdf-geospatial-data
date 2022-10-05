#!/bin/bash

# ==========================================================================
# ==========================================================================
# prefect 2.0


base_directory=$(awk -F "=" '/base_directory/ {print $2}' config.ini)

prefect_dir=${base_directory}/.prefect

mkdir ${prefect_dir}

export PREFECT_HOME=${prefect_dir}
export PREFECT_LOCAL_STORAGE_PATH=${PREFECT_HOME}/storage
export PREFECT_ORION_DATABASE_CONNECTION_URL="sqlite+aiosqlite:///${PREFECT_HOME}/orion.db"


# the below are all optional and only required if you wish to manually setup your dask cluster and utilize the prefect UI

# in all terminals, first run the below commands
#   - activating the conda env in these terminals is necessary for access to prefect/dask commands
#   - adding the project directory to the conda path and being in the project directory are necessary to ensure local modules can be imported by the dask workers
#        + using conda develop and being in the project directory may be redundant
conda activate china_osm
cd ${base_directory}
conda develop .


# terminal 1 (uncomment to run prefect ui - ui accessible via http://127.0.0.1:4200/)
# prefect orion start

# terminal 2 (optional - uncomment only if implementing your own prefect queues)
# prefect agent start -q "china-osm-queue"


# terminal 3 (uncomment if using manually created dask cluster)
# dask-scheduler

# terminal 4 (uncomment/adjust on of the below lines if using manually created dask cluster)
# dask-worker tcp://127.0.0.1:8786 --nprocs 4 --memory-limit 4GB
# dask-worker tcp://127.0.0.1:8786 --nprocs 10 --nthreads 1





# # ==========================================================================
# # ==========================================================================
# # DEPRECATED - for use with Prefect 1.0 only

# # ==========================================================================
# # ==========================================================================
# # prefect 1.0
#
# # python environment used for prefect agent/server
# # must have prefect and docker-compose packages installed

# # =====================================
# # in termal 1


# # ---------------------------
# # if using prefect cloud, use the following to authenticate, create a project, and start your local agent
# # also copy your prefect key to a ".prefect_key" file in the root directory of this repo


# # set backend to cloud if using cloud management
# prefect backend cloud

# # auth
# prefect_key=`cat .prefect_key`
# prefect auth login --key $prefect_key


# # ---------------------------
# # if using local server

# # first start docker desktop
# # https://docs-v1.prefect.io/orchestration/server/deploy-local.html#ui-configuration

# # in terminal 1
# prefect backend server


# prefect server start --expose


# # =====================================
# # in terminal 2

# # create project
# prefect create project china-osm

# # start agent
# prefect agent local start

# # example for starting local agent in python (not needed if using above commands)
# # state = flow.run_agent(log_to_cloud=True, show_flow_logs=True)


# # ---------------------------
# # if using a full dask cluster

# # =====================================
# # in terminal 3
# dask-scheduler

# # =====================================
# # in terminal 4
# # dask-worker tcp://127.0.0.1:8786 --nprocs 4 --memory-limit 4GB
# dask-worker tcp://127.0.0.1:8786 --nprocs 10


# # example for starting dask client in python (not needed if using above commands)
# # from dask.distributed import Client
# # client = Client('tcp://127.0.0.1:8786')

