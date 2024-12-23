
## Multi-Tier Storage System Simulation Project
# Overview: 
   The HPC Multi-Tier Storage Simulator is a Python-based simulation tool designed to evaluate and compare various cache eviction policies within multi-tier High-Performance Computing (HPC) storage systems. The simulator models heterogeneous storage architectures, specifically focusing on Solid-State Drives (SSDs) and Hard Disk Drives (HDDs), to analyze the performance and efficiency of different data placement and eviction strategies

# Features
Two-Tier Storage Modeling: Simulates a high-performance SSD tier and a lower-performance HDD tier with configurable characteristics.



# Multi-Criteria Decision Making:

   Implements Weighted Sum Model (WSM) and Technique for Order Preference by Similarity to Ideal Solution (TOPSIS) for aggregating multiple eviction criteria in MC-ARC.
# File Lifetime Prediction:

   Integrates file lifetime predictions using Convolutional Neural Networks (CNNs) to inform eviction decisions.
# User Fairness:

   Ensures equitable allocation of high-performance tier resources among multiple users.
## Comprehensive Metrics:

I/O Processing Time
Hit Ratio
Throughput per User
## Realistic Workload Support:

Supports workloads derived from real HPC applications, including Yombo, Synthesized Google I/O Traces, and IBMObjectStoreTrace

# The policies implemented are: 

#### Block-Based Policies:
        Least Recently Used (LRU),
        Least Frequently Used (LFU),
        Adaptive Replacement Cache (ARC)
#### File-Based Policy:
        Multicriteria ARC (MC-ARC)
               
    
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

**Note:** The current directory is mounted inside the container, which allows you to see changes without needing to rebuild the Docker image.
