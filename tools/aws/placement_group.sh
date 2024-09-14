
pl=$(aws ec2 describe-placement-groups --filters "Name=group-name,Values=DistributedStreaming" \
                                  --query "PlacementGroups[*].{ID:GroupId}" \
                                  | egrep "ID" | awk '{print $NF}' \
                                  | sed 's/\"//g;s/\,//')

# Create placement group if it doesn't exist
if [ -z $pl ]; then
  aws ec2 create-placement-group --group-name DistributedStreaming \
                                 --strategy cluster
fi
