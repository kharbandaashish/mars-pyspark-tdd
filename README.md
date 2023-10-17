# Mars Petcare: Pyspark interview question

## Setup to run locally
1. **Install miniconda**  
    a. Here (https://docs.conda.io/en/latest/miniconda.html#) is a list of installers for each operating system. Download and install the one you're on. Note:  
        i. If you're using Windows make sure you choose the "Install for Just Me" option.  
        ii. You want to add conda to PATH variables for easier use later.  
    b. After the installation is successful you can test this by running `conda` in a terminal.  
2. **Local setup**  
    a. Create a new conda environment and activate it. Allow all new packages to be installed. This will setup a new development  
        `conda create -n <your-environment-name> python=3.9`  
        `conda activate <your-environment-name>`  
    b.  Install project requirements  
        `pip install -r requirements.txt`  
    c.  Install java8 in your conda environment
        `conda install -c anaconda "openjdk=8.0.152"`# mars-pyspark-tdd
