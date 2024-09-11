
if [[ $# -ne 2 ]]; then
  echo "Invalid arguments. Require instance-id, subnet"
  echo "instance-id:   The instance ID of the main node machine."
  echo "subnet:        The subnet of the main node machine."
  exit 1
fi
instance=$1
subnet=$2

# Get the current ec2 volume
vol_id=$(aws ec2 describe-volumes --filters "Name=tag:Name,Values=LandscapeData"\
                         --query "Volumes[*].{ID:VolumeId}" \
                         | egrep "ID" | awk '{print $NF}' \
                         | sed 's/\"//g;s/\,//')

# If the datasets volume doesn't exist, create it
if [ -z $vol_id ]; then
  aws ec2 create-volume --volume-type gp3 \
              --size 128 \
              --iops 5000 \
              --throughput 256 \
              --availability-zone $subnet \
              --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=LandscapeData}]"
  sleep 5 # wait for volume to be available

  # Get the current ec2 volume
  vol_id=$(aws ec2 describe-volumes --filters "Name=tag:Name,Values=LandscapeData"\
                           --query "Volumes[*].{ID:VolumeId}" \
                           | egrep "ID" | awk '{print $NF}' \
                           | sed 's/\"//g;s/\,//')

  if [ -z $vol_id ]; then
    echo "ERROR: Couldn't create datasets volume???"
    exit 1
  fi
  # Mount the volume to the main node
  aws ec2 attach-volume --volume-id $vol_id \
                        --instance-id $instance \
                        --device /dev/sdf

  # Format the volume
  sleep 5 # wait for volume to be ready
  sudo mkfs -t ext4 /dev/sdf
else
  # Mount the volume to the main node
  aws ec2 attach-volume --volume-id $vol_id \
                        --instance-id $instance \
                        --device /dev/sdf
  sleep 5 # wait for volume to be ready
fi

sudo mkdir /mnt/ssd1
sudo mount -t ext4 /dev/sdf /mnt/ssd1
sudo chown -R ec2-user /mnt/ssd1
echo "Dataset volume created and mounted!"
