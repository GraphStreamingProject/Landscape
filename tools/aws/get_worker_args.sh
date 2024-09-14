
if [[ $# -ne 2 ]]; then
  echo "Invalid arguments. Require region, subnet"
  echo "region:     Ec2 region in which main node lives."
  echo "subnet:     Subnet of the main node."
  exit
fi

region=$1
subnet=$2

pl=$(aws ec2 describe-placement-groups --filters "Name=group-name,Values=DistributedStreaming" \
                                  --query "PlacementGroups[*].{ID:GroupId}" \
                                  | egrep "ID" | awk '{print $NF}' \
                                  | sed 's/\"//g;s/\,//')

worker_sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=WorkerRules" \
                                 --query "SecurityGroups[*].{ID:GroupId}" \
                                 | egrep "ID" | awk '{print $NF}' \
                                 | sed 's/\"//g;s/\,//')

subnet=$(aws ec2 describe-subnets --region $region \
                                  --filters "Name=availability-zone, Values=$subnet" \
                                  --query "Subnets[*].{ID:SubnetId}" \
                                  | egrep "ID" | awk '{print $NF}' \
                                  | sed 's/\"//g;s/\,//')

echo "--placement_group_id=$pl --subnet_id=$subnet --security_groups $worker_sg"
