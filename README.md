# Landscape
Linear sketching for the connected components and k-edge connectivity problems. Landscape distributes the CPU intensive work of performing sketch updates to many worker nodes while keeping sketch data on the main node. The result is that we can process graph update streams at near sequential RAM bandwidth speeds.

## Using Landscape
Landscape is a c++ library built with cmake. You can easily use Landscape in your code through cmake with FetchContent or ExternalProjectAdd.

## Reproducing Our Experiments on EC2
Landscape appears in [ALENEX'25](). You can reproduce our experimental results by following these instructions.

1. Create an AWS Secret Key. `IAM->Users->YourUsername->Security credentials`
2. Provision the Main Node on EC2. Launch a `c5n.18xlarge` EC2 instance. You will need to generate an ssh keypair through the EC2 interface as part of launching the instance. Call this key `Creation_Key`.
3. Download the ssh key you generated.
4. Upload the ssh keypair to the main node. `rsync -ve "ssh -i </path/to/key>" </path/to/key> ec2-user@<public-dns-addr-of-main>:~/.ssh/id_rsa` You can see the public dns address of the main node under instance details.
5. Connect to the main node. `ssh -i <path/to/key> ec2-user@<public-dns-addr-of-main>`
6. Install packages
```
sudo yum update -y
sudo yum install -y tmux git
```
7. Clone this repository. IMPORTANT: Ensure the repository is cloned to the ec2-user home directory and that the name is unchanged. `~/Landscape`
8. From `~\Landscape` run `./runme.sh`. This script will prompt you to agree to the use of sudo commands and to enter your aws secret key and default region. 
