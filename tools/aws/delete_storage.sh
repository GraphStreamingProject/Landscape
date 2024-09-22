
# Get the current ec2 volume
vol_id=$(aws ec2 describe-volumes --filters "Name=tag:Name,Values=LandscapeData"\
                         --query "Volumes[*].{ID:VolumeId}" \
                         | egrep "ID" | awk '{print $NF}' \
                         | sed 's/\"//g;s/\,//')

if [ ! -z $vol_id ]; then
  echo "ERROR: Datasets volume doesn't exist?"
  exit
fi

aws ec2 delete-volume --volume-id $vol_id
