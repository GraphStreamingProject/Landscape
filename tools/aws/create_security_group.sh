
main_sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=MainRules" \
                                 --query "SecurityGroups[*].{ID:GroupId}" \
                                 | egrep "ID" | awk '{print $NF}' \
                                 | sed 's/\"//g;s/\,//')

worker_sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=WorkerRules" \
                                 --query "SecurityGroups[*].{ID:GroupId}" \
                                 | egrep "ID" | awk '{print $NF}' \
                                 | sed 's/\"//g;s/\,//')

# Create the security groups if they don't exist
if [ -z $main_sg ]; then
  aws ec2 create-security-group --group-name MainRules \
                                --description "Rules for Main Node"

  sleep 5 # wait for sg to be available
  main_sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=MainRules" \
                                 --query "SecurityGroups[*].{ID:GroupId}" \
                                 | egrep "ID" | awk '{print $NF}' \
                                 | sed 's/\"//g;s/\,//')
fi

if [ -z $worker_sg ]; then
  aws ec2 create-security-group --group-name WorkerRules \
                                --description "Only allows traffic to and from main group"

  sleep 5 # wait for sg to be available
  worker_sg=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=WorkerRules" \
                                 --query "SecurityGroups[*].{ID:GroupId}" \
                                 | egrep "ID" | awk '{print $NF}' \
                                 | sed 's/\"//g;s/\,//')
fi

aws ec2 authorize-security-group-ingress --group-id $main_sg --protocol tcp --port 22 --cidr 0.0.0.0/0
aws ec2 authorize-security-group-ingress --group-id $main_sg --protocol all --source-group $worker_sg

aws ec2 authorize-security-group-ingress --group-id $worker_sg --protocol all --source-group $main_sg
