# Landscape
Linear sketching for the connected components and k-edge connectivity problems. Landscape distributes the CPU intensive work of performing sketch updates to many worker nodes while keeping sketch data on the main node. The result is that we can process graph update streams at near sequential RAM bandwidth speeds.

[![DOI](https://zenodo.org/badge/456755512.svg)](https://zenodo.org/doi/10.5281/zenodo.13826257)

## Using Landscape
Landscape is a c++ library built with cmake. You can easily use Landscape in your code through cmake with FetchContent or ExternalProjectAdd.
Requirements
- OS: Unix (Not Mac): Tested on Amazon Linux and Ubuntu
- cmake >= 3.16
- openmpi 4.1.3
- c++14

## Reproducing Our Experiments on EC2
Landscape appears in [ALENEX'25](). You can reproduce our paper's experimental results by following these instructions. You will need access to an AWS account with roughly $60 in credits.

1. Create an AWS Secret Key. `IAM->Users->YourUsername->Security credentials`. Make note of the access key and secret key.
2. Provision the Main Node on EC2. `EC2->Instances->Launch instances`
   - Select the Amazon Linux 2023 AMI. (That is, not Amazon Linux 2 AMI)
   - Choose `c5n.18xlarge` as the instance type.
   - Create a new key pair. Select `RSA` and call this key `Creation Key`. The name must match exactly. (If you have already created this key pair then skip this step)
   - Select `Creation Key` as the key pair.
   - Under Advanced details select create new placement group. Call the group `DistributedStreaming` and select `Cluster` as the placement strategy. The name must match exactly. (If you have already created this placement group then skip this step)
   - Select the `DistributedStreaming` placement group.
4. Upload the ssh keypair to the main node. `rsync -ve "ssh -i </path/to/key>" </path/to/key> ec2-user@<public-dns-addr-of-main>:~/.ssh/id_rsa`
   - Find the public dns address `EC2->Instances->click instance->PublicIPv4 DNS`.
5. Connect to the main node. `ssh -i <path/to/key> ec2-user@<public-dns-addr-of-main>`
6. Install packages
```
sudo yum update -y
sudo yum install -y tmux git
```
7. Clone this repository. IMPORTANT: Ensure the repository is cloned to the ec2-user home directory and that the name is unchanged. `~/Landscape`
8. From `~\Landscape` run `bash runme.sh`. This script will prompt you for the following:
   - Agree to the use of sudo commands
   - Choose whether to run the `full` experiments (all datapoints) or `limited` experiments (fewer datapoints per experiments)
   - Enter your aws secret key and default EC2 region (this should be the region in which the main node was created)
9. After the experiments conclude, copy `figures.pdf` from `~/Landscape` to your personal computer. You can acomplish this by running: `rsync -ve "ssh -i ~/.ssh/Creation_Key.pem" ec2-user@<publis-dns-addr-of-main>:~/Landscape/figures.pdf .` on your personal computer.
10. Terminate the main node in EC2
