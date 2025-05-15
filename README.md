
## Multi-Tier Storage System Simulation Project
# Overview: 
   The HPC Multi-Tier Storage Simulator is a Python-based simulation tool designed to evaluate and compare various cache eviction policies within multi-tier High-Performance Computing (HPC) storage systems. The simulator models heterogeneous storage architectures, specifically focusing on Solid-State Drives (SSDs) and Hard Disk Drives (HDDs), to analyze the performance and efficiency of different data placement and eviction strategies



# The policies implemented are: 

#### Block-Based Policies:
        Least Recently Used (LRU),
        Least Frequently Used (LFU),
        Adaptive Replacement Cache (ARC)
#### File-Based Policy:
        A Multicriteria File-Level Placement Policy for HPC Storage (MC-ARC)
               
## Comprehensive Metrics:

        I/O Processing Time (S)
        Hit Ratio (%)
        Throughput per User (B/S)   
# Requirements:   
        python 3.7
        pandas  1.1.3
        matplotlib  3.3.2
        numpy  1.19.2
        
# Installation:
        pip install pandas
        pip install matplotlib
        pip install numpy
        Download the project: mc_arc
 
  # Usage:  
  You can run this project using two methods:

  ## Method 1: Running Locally (Without Docker)
    1. Clone the project from the git repository :
       """
          git clone https://github.com/hocinemahni/mc_arc.git
          cd mc_arc
       """
    2. Install the requirements
        """
           pip install -r requirements.txt
        """
    3. configure ssd_tier and hdd_tier in the main.py file
    4. configure the cache_size_proportions in the main.py file
    5. configure path to the metadata in the main.py file
    6. configure path to the data file in the main.py file 
    7. Run the main.py file with the command below
       """
          python main.py
       """
    8. The results will be saved in the graphes folder
            
  ## Method 2: Running with Docker
  ![Docker](utils/Docker.png)

       If you prefer to run the project in a Docker container, follow these steps:

          1. Ensure Docker and Docker Compose are installed:

             - Download Docker from [Docker's official website](https://www.docker.com/products/docker-desktop).
             - Docker Compose usually comes with Docker Desktop, but you can check installation instructions [here](https://docs.docker.com/compose/install/).

          2. Build the Docker image:

              Open a terminal in the root of your project and run:

                  """
                     docker-compose build
                  """

          3. Run the Docker container:

              Once the image is built, start the container with:

                 """
                    docker-compose up
                 """

          4. Stop the Docker container:

              If you need to stop the application, use:

                  """
                      docker-compose down
                  """
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
## Citation

If you use this simulator, **please cite the following article**:


@inproceedings{10.1145/3672608.3707969,
author    = {Mahni, Hocine and Rubini, St{\'e}phane and Gougeaud, S{\'e}bastien and Deniel, Philippe and Boukhobza, Jalil},
title     = {Multicriteria File-Level Placement Policy for HPC Storage},
booktitle = {Proceedings of the 40th ACM/SIGAPP Symposium on Applied Computing},
series    = {SAC~'25},
year      = {2025},
pages     = {1399--1406},
publisher = {Association for Computing Machinery},
address   = {New York, NY, USA},
isbn      = {9798400706295},
doi       = {10.1145/3672608.3707969},
url       = {https://doi-org.ins2i.bib.cnrs.fr/10.1145/3672608.3707969},
abstract  = {The rapid expansion of data volumes across various scientific and technical fields, along with the development of exascale computing in the high performance computing (HPC) domain, continually challenge existing storage systems. These systems typically consist of heterogeneous multi-tier storage architectures, ranging from high-speed solid-state drives (SSDs) tier with limited storage capacity to slower magnetic tapes tier with larger storage capacity. A significant challenge in HPC storage systems is the effective placement and migration of data across different storage levels. Current strategies, such as those implemented in parallel file systems like Lustre, utilize hierarchical storage management (HSM) solutions such as the Robinhood Policy Engine, which operate at the file granularity level for data eviction policies. In contrast, traditional caching policies work at the block level. This mismatch of granularity makes it difficult to adopt traditional eviction policies to those HSM. This study introduces a new multi-criteria file-level eviction policy incorporating frequency and recency of access, file lifetime, and a fairness criterion. Our policy reduces I/O processing times by average of 30\% for tested workloads and improves the hit ratio by 56.43\% on average, outperforming block-based cache replacement policies such as LRU, LFU, and ARC.},
keywords  = {data placement, multi-tier storage, high performance computing, ARC, HSM, storage cache, eviction policy},
location  = {Catania International Airport, Catania, Italy},
numpages  = {8}
}
---------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------
“We are therefore forced to recognize the possibility of constructing a hierarchy of memories,
each of which has greater capacity than the preceding but which is less quickly accessible.”
— In Preliminary Discussion of the Logical Design of Electronic Computing Instrument, 28 June 1946
