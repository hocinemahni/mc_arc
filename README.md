
## Multi-Tier Storage System Simulation Project
# Overview: 
    This project focuses on simulating a multi-tier storage system, with an emphasis on data management through the implementation of various placement policies. 
    The goal is to explore how different strategies can affect the performance of the storage system.

# The policies implemented are: 
        ARC, 
        CFS-ARC, 
        FG-ARC, 
        BFH_ARC_alpa_beta, 
        BFH_ARC_whithout_alpha_beta, 
        BFH_ARC_whith_size, 
        RLT_ARC, 
        Idle_Time_BFH_ARC_policy    
               
    
# Requirements:   
        python 3.7
        pandas  1.1.3
        matplotlib  3.3.2
        numpy  1.19.2
        
# Installation:
        pip install pandas
        pip install matplotlib
        pip install numpy
        Download the project: Arc_file_simu
 
  # Usage:  
  You can run this project using two methods:

  ## Method 1: Running Locally (Without Docker)
    1. Clone the project from the git repository :
       '''
          git clone https://github.com/hocinemahni/Arc_file_simu.git
          cd Arc_file_simu
       '''
    2. Install the requirements
        '''
           pip install -r requirements.txt
        '''
    3. configure ssd_tier and hdd_tier in the main.py file
    4. configure the cache_size_proportions in the main.py file
    5. configure path to the metadata in the main.py file
    6. configure path to the data file in the main.py file 
    7. Run the main.py file with the command below
       '''
          python main.py
       '''
    8. The results will be saved in the graphes folder
            
  ## Method 2: Running with Docker
  ![Docker](utils/Docker.png)

       If you prefer to run the project in a Docker container, follow these steps:

          1. Ensure Docker and Docker Compose are installed:

             - Download Docker from [Docker's official website](https://www.docker.com/products/docker-desktop).
             - Docker Compose usually comes with Docker Desktop, but you can check installation instructions [here](https://docs.docker.com/compose/install/).

          2. Build the Docker image:

              Open a terminal in the root of your project and run:

                  '''
                     docker-compose build
                  '''

          3. Run the Docker container:

              Once the image is built, start the container with:

                 '''
                    docker-compose up
                 '''

          4. Stop the Docker container:

              If you need to stop the application, use:

                  '''
                      docker-compose down
                  '''

**Note:** The current directory is mounted inside the container, which allows you to see changes without needing to rebuild the Docker image.
