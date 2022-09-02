#!/bin/bash

# ==========================================================================
# ==========================================================================
# prefect 2.0

# prefect setup
pip install prefect==2.2.0 prefect-dask bokeh>=2.1.1

# terminal 1
prefect orion start

# terminal 2
prefect agent start -q "china-osm-queue"

# terminal 3
dask-scheduler

# terminal 4
# dask-worker tcp://127.0.0.1:8786 --nprocs 4 --memory-limit 4GB
dask-worker tcp://127.0.0.1:8786 --nprocs 10



# ==========================================================================
# ==========================================================================
# prefect 1.0

# python environment used for prefect agent/server
# must have prefect and docker-compose packages installed

# =====================================
# in termal 1


# ---------------------------
# if using prefect cloud, use the following to authenticate, create a project, and start your local agent
# also copy your prefect key to a ".prefect_key" file in the root directory of this repo


# set backend to cloud if using cloud management
prefect backend cloud

# auth
prefect_key=`cat .prefect_key`
prefect auth login --key $prefect_key


# ---------------------------
# if using local server

# first start docker desktop
# https://docs-v1.prefect.io/orchestration/server/deploy-local.html#ui-configuration

# in terminal 1
prefect backend server


prefect server start --expose


# =====================================
# in terminal 2

# create project
prefect create project china-osm

# start agent
prefect agent local start

# example for starting local agent in python (not needed if using above commands)
# state = flow.run_agent(log_to_cloud=True, show_flow_logs=True)



# ---------------------------
# if using a full dask cluster

# =====================================
# in terminal 3
dask-scheduler

# =====================================
# in terminal 4
# dask-worker tcp://127.0.0.1:8786 --nprocs 4 --memory-limit 4GB
dask-worker tcp://127.0.0.1:8786 --nprocs 10



# example for starting dask client in python (not needed if using above commands)
# from dask.distributed import Client
# client = Client('tcp://127.0.0.1:8786')

