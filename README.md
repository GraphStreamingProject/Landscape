# DistributedStreamingCC
A distributed extension to GraphZeppelin (GraphStreamingCC)

## Cluster Setup
Ansible files for setting up the cluster are found under `tools/ansible`.  
Ansible commands are run with `ansible-playbook -i /path/to/inventory.ini /path/to/<script>.yaml`.

### 1. Install useful packages
```
sudo yum update -y
sudo yum install -y tmux htop git
sudo amazon-linux-extras install -y ansible2
```

### 2. Install cmake version 3.16+
```
wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc2/cmake-3.23.0-rc2-linux-x86_64.sh
sudo mkdir /opt/cmake
sudo sh cmake-3.23.0-rc2-linux-x86_64.sh --prefix=/opt/cmake
sudo ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake
```
When running cmake .sh script enter y to license and n to install location.  
These commands install cmake version 3.23 but any version >= 3.16 will work.

### 3. Create and populate inventory.ini and hostfile
Place both of these files in your home directory
* Example inventory.ini
```
[head]
ip-172-31-75-183
[workers]
ip-172-31-73-198.ec2.internal
ip-172-31-69-241.ec2.internal
```
* Example hostfile  
Here the first entry is the main node and we restrict it to only running a single MPI process. This ensures all the workers are running on the worker nodes.
```
ip-172-31-75-183 slots=1 max_slots=1
ip-172-31-73-198
ip-172-31-69-241
```

### 4. Setup ssh keys
* Copy EMR.pem to cluster `rsync -ve "ssh -i </path/to/EMR.pem>" </path/to/EMR.pem> <AWS-user>@<main_node_dns_addr>`
* Ensure key being used is default rsa key for ssh `id_rsa` for example `cp EMR.pem ~/.ssh/id_rsa`

### 5. Clone and build repo
* clone
* make `build` directory in project repo
* run `cmake .. ; make` in build directory

### 6. Distribute ssh keys to cluster
* Run ansible file `ssh.yaml`
* Ensure you can ssh to the workers from the main node and back

### 7. Install MPI on nodes in cluster
* Run ansible script `mpi.yaml`
### 8. Distribute executables and hostfile to worker nodes
* Build the executables `cmake .. ; make`
* Run ansible script `files.yaml`

After running these steps you should be able to run the unit tests across the cluster with the command
```
mpirun -np 4 -hostfile ~/hostfile ./distrib_tests
```
-np denotes the number of processes to run

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
