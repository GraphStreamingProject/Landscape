
# Get the current ec2 volume
aws ec2 describe-volumes --filters "Name=tag:Name,Values=LandscapeData"  \
                      | egrep "ID" | awk '{print $NF}' \
                      | sed 's/\"//g;s/\,//' > vol_id

if [ ! -z $vol_id ]; then
  echo "ERROR: Datasets volume doesn't exist?"
  exit
fi

aws ec2 delete-volume --volume-id $vol_id
