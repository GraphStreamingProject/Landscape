# DistributedStreamingCC
A distributed extension to GraphZeppelin (GraphStreamingCC)

## Running experiments
1. If the stream lives in a file, ensure that the file has been brought into the file cache before beginning the experiment. One way to do this is `cat 'stream_file' > /dev/null`
2. pass `-bind-to none` as an additional option to mpirun. For example `mpirun -np 129 -hostfile hostfile -bind-to none ./speed_expr ...`
3. You can monitor the status of the cluster by, in a seperate window, running the command `watch -n 1 cat cluster_status.txt`

## Cluster Setup (Automatic)
### 0. Ensure the master is able to read IPS
There is an IAM Role that allows the EC2 instance to read IPS. This is used to automatically get the IPS.
![image](https://user-images.githubusercontent.com/4708326/164508403-70fbb271-fa4c-4145-9093-ff86320e1bba.png)

### 1. Tag the Master and Worker nodes on setup

The script only reads properly tagged EC2 instances. The Master must be tagged 'ClusterNodeType:Master' to appear at the top of the host files. The Workers must be tagged 'ClusterNodeType:Worker'.

![image](https://user-images.githubusercontent.com/4708326/164511717-02f2feee-a9f8-4b04-a35e-fb53be5140ee.png)

### 2. Properly Provision the Master
See Manual instructions for installing ansible and the repo. For the sake of these instructions, we assume that the Master is already provisioned and setup.

### 3. Run the automated script
Navigate to `DistributedStreamingCC/tools` and run `./setup_tagged_workers.sh <cpus_per_node>`.
This will perform all of the steps in the Manual Setup below, including parallel ansible commands, generating the hostfiles, etc.

You will be prompted to answer 'Y' or 'N' if the hostfile and inventory look correct or not. Answering 'N' will abort the process.

## Cluster Setup (Manual)
Ansible files for setting up the cluster are found under `tools/ansible`.  
Ansible commands are run with `ansible-playbook -i /path/to/inventory.ini /path/to/<script>.yaml`.  

### 1. Install useful packages
```
sudo yum update -y
sudo yum install -y tmux htop git gcc-c++ jq
sudo amazon-linux-extras install -y ansible2
```

### 2. Install cmake version 3.16+
First Step:
#### x86_64
```
wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc2/cmake-3.23.0-rc2-linux-x86_64.sh
sudo mkdir /opt/cmake
sudo sh cmake-3.23.0-rc2-linux-x86_64.sh --prefix=/opt/cmake
```
#### aarch64
```
wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc5/cmake-3.23.0-rc5-linux-aarch64.sh
sudo mkdir /opt/cmake
sudo sh cmake-3.23.0-rc5-linux-aarch64.sh --prefix=/opt/cmake
```
Second Step:
```
sudo ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake
```
When running cmake .sh script enter y to license and n to install location.  
These commands install cmake version 3.23 but any version >= 3.16 will work.

### 3. Create a `node_list.txt` file
The `node_list.txt` file is a list of the private dns addresses for every node in the cluster. The main node should be the first dns address listed followed by the worker dns addresses

Example:
```
ip-172-31-1-128.ec2.internal
ip-172-31-1-129.ec2.internal
ip-172-31-1-247.ec2.internal
ip-172-31-1-248.ec2.internal
```
Here the first entry `ip-172-31-1-128.ec2.internal` is the main node of this cluster.

### 4. Setup ssh keys
* Copy EMR.pem to cluster `rsync -ve "ssh -i </path/to/EMR.pem>" </path/to/EMR.pem> <AWS-user>@<main_node_dns_addr>:.`
* Ensure key being used is default rsa key for ssh `id_rsa` for example `cp EMR.pem ~/.ssh/id_rsa`

### 5. Clone DistributedStreamingCC Repo

### 6. Run `cluster_basic_init.sh`
This simple bash script will read from the node_list.txt file to construct the ansible `inventory.ini` file and the MPI `hostfile`. It should be run from your home directory. THe arguments to specify to the script is the path to the node_list.txt file and the number of cpus per worker node.

Example:
```
bash DistributedStreamingCC/tools/cluster_basic_init.sh node_list.txt 16
```
The script will automatically set the known_hosts for all the machines in the cluster to whatever ssh-keyscan finds (this is a slight security issue if you don't trust the cluster but should be fine as we aren't transmitting sensative data). It will additionally confirm with you that the `inventory.ini` and `hostfile` it creates look reasonable.

### 6. Distribute ssh keys to cluster
* Run ansible file `ssh.yaml` with `ansible-playbook -i inventory.ini DistributedStreamingCC/tools/ansible/ssh.yaml`

### 7. Install MPI on nodes in cluster
* Run ansible file `mpi.yaml` with `ansible-playbook -i inventory.ini DistributedStreamingCC/tools/ansible/mpi.yaml`
* Run `source ~/.bashrc` in open terminal on main node

### 8. Build Distributed Streaming Repo
* make `build` directory in project repo
* run `cmake .. ; make -j` in build directory

### 9. Distribute executables and hostfile to worker nodes
*  Run ansible file `files.yaml` with `ansible-playbook -i inventory.ini DistributedStreamingCC/tools/ansible/files.yaml`

### 10. (Optional) Install EFA
* Follow the instructions at https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html#efa-start-security

After running these steps you should be able to run the unit tests across the cluster with the command
```
mpirun -np 4 -hostfile ~/hostfile ./distrib_tests
```
-np denotes the number of processes to run

## Amazon Storage
### EBS Storage
EBS disks are generally found installed at `/mnt/nvmeXnX` where X is the disk number. In order to use them, the disk must be formatted and then mounted.
* `sudo lsblk -f` to list all devices
* (Optional) If no filesystem exists on the device than run `sudo mkfs -t xfs /dev/<device>` to format the drive. This will overwrite the content on the device so DO NOT do this if a filesystem already exists.
* Create the mount point directory `sudo mkdir /mnt/<mnt_point>`
* Mount the device `sudo mount /dev/<device> /mnt/<mnt_point>`
* Adjust owner and permissions of mount point `sudo chown -R <user> /mnt/<mnt_point>` and `chmod a+rw /mnt/<mnt_point>` 

## Single Machine Setup

### 1. Install OpenMPI
For Ubuntu the following will install openmpi
```
sudo apt update
sudo apt install libopenmpi-dev
```
Google is your friend for other operating systems :)

### 2. Run executables
Use the `mpirun` command to run mpi programs. For example, to run the unit tests with 4 processes, the following command is used.
```
mpirun -np 4 ./distrib_tests
```

## Tips for Debugging with MPI
If you want to run the code using a debugging tool like gdb you can perform the following steps.
1. Compile with debugging flags `cmake -DCMAKE_BUILD_TYPE=Debug .. ; make`
2. Launch the mpi task with each process in its own window using xterm `mpirun -np <num_proc> term -hold -e gdb <executable>`

Print statement debugging can also be helpful, as even when running in a cluster across many machines, all the output to console across the workers is printed out by the main process. 
