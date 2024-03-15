
## Multi-Tier Storage System Simulation Project
# Overview: 
    This project focuses on simulating a multi-tier storage system, 
    with an emphasis on data management through the implementation of various placement policies. 
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
        1. Clone the project from the git repository
        2. Install the requirements
        3. configure ssd_tier and hdd_tier in the main.py file
        4. configure the cache_size_proportions in the main.py file
        5. configure path to the metadata in the main.py file
        6. configure path to the data file in the main.py file 
        7. Run the main.py file with the command below
        8. The results will be saved in the graphes folder
