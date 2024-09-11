
if [[ $# -ne 2 ]]; then
  echo "Invalid arguments. Require instance-id, subnet"
  echo "instance-id:   The instance ID of the main node machine."
  echo "subnet:        The subnet of the main node machine."
  exit
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
              --availability-zone $subnet \
              --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=LandscapeData}]"
fi

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

sudo mkdir /mnt/ssd1
sudo mount /dev/sdf
sudo chown -R ec2-user /mnt/ssd1
