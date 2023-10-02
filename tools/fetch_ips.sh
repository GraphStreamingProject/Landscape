#!/bin/bash

if [[ $# -ne 1 ]]; then
  echo "Invalid arguments. Require region"
  echo "region:  The EC2 region where we have machines."
  exit
fi

region=$1
aws ec2 describe-instances --region $region --no-paginate --filters "Name=instance-state-name,Values=running" "Name=tag:ClusterNodeType,Values=Master" | jq -r .Reservations[].Instances[].NetworkInterfaces[0].PrivateDnsName
aws ec2 describe-instances --region $region --no-paginate --filters "Name=instance-state-name,Values=running" "Name=tag:ClusterNodeType,Values=Worker" | jq -r .Reservations[].Instances[].NetworkInterfaces[0].PrivateDnsName
