# Media News Text Classifier [Prototype]
# 1. Setting-up environment

Select the OS you are installing the environment for: 
- **EC2 (with Linux)** - execute steps as described in **Section 1.1** 
- **Windows** - execute steps as described in **Section 1.2**

As a pre-requisites:
- **SSH key** generated and added to github (so to be able to clone the repo)
    - Generate SSH key pair
        - `cd ~`
        - `ssh-keygen -t rsa -b 4096 -C "your_name@gmail.com"`
        - `Save key to: ~/.ssh/id_rsa_projectx` >>> `yes`
        - No passphrase used
    - Start the ssh-agent in the background
       - `eval "$(ssh-agent -s)"`
    - Add your SSH private key to the ssh-agent
       - `ssh-add ~/.ssh/id_rsa_projectx`


## 1.2 Windows (local PC)

### 1.2 Install Anaconda
Find the most recent Anaconda's Linux distro in https://repo.anaconda.com/archive/ (I am using Anaconda3-2021.11-Windows-x86_64.exe).

1. Download and install `https://repo.anaconda.com/archive/Anaconda3-2021.11-Windows-x86_64.exe`
   - In my case, I did install Anaconda to `C:\Users\Nazar\Anaconda3`
2. Open environmental variables on your Windows machine and make sure your `Path` env variable contains the following:
   - `C:\Users\Nazar\Anaconda3\Scripts\windows` 
   - `C:\Users\Nazar\Anaconda3\Library\`
   - `C:\Users\Nazar\Anaconda3\Library\bin\`
   - `C:\Users\Nazar\Anaconda3\Library\mingw-w64\bin\`

### 1.2.3. Create Conda Environment

For the majority of the use cases of this repo, create **light weight** conda environment (mainly for EDA, data wrangling, ...). 
In Windows PowerShell:
   1. `cd C:\ProjectX\workspace\project_x`
   2. `conda activate base`
   3. `conda install nb_conda_kernels -y`
   4.  If the **environment already exists**: 
       - `conda env remove -n py38eda`
   5. `conda clean --all -y` 
   6. `conda env create -f environment_py38eda_windows.yml`
   7. `conda activate py38eda` 

For the heavy-weight work like NER, Geocoding, one needs to install the following conda environment.
In Windows PowerShell:
   1. `cd ~/workspace/project_x`
   2. `conda activate base`
   3. `conda install nb_conda_kernels -y`
   4.  If the **environment already exists**: 
       - `conda env remove -n py38dnn`
   5. `conda clean --all -y` 
   6. `conda env create -f environment_py38dnn_windows.yml`
   7. `conda activate py38dnn` 
   8. To install **Spacy (GPU-enabled)**:
      - `conda install -c pytorch torchvision==0.11.3 pytorch==1.10.2 cudatoolkit==11.3.1 -y`
      - `conda install -c conda-forge cupy==10.1.0 spacy==3.2.1 spacy-transformers==1.1.5 -y`
   9. Install **pre-trained Spacy-models**:
      - `python -m spacy download en_core_web_lg`
      - `python -m spacy download en_core_web_trf`
   10. Install **Prodigy** (optional, needed for NER):
      - `cd C:\ProjectX\workspace`
      - If not exists:
        - `mkdir prodigy`
      - `cd C:\ProjectX\workspace\prodigy\prodigy_source\1.11.7\windows`
      - `pip install prodigy-1.11.7-cp38-cp38-win_amd64.whl`
