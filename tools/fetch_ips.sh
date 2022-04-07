#!/bin/bash
aws ec2 describe-instances --region us-east-1 --no-paginate --filters "Name=instance-state-name,Values=running" "Name=tag:ClusterNodeType,Values=Master" | jq -r .Reservations[].Instances[].NetworkInterfaces[0].PrivateDnsName
aws ec2 describe-instances --region us-east-1 --no-paginate --filters "Name=instance-state-name,Values=running" "Name=tag:ClusterNodeType,Values=Worker" | jq -r .Reservations[].Instances[].NetworkInterfaces[0].PrivateDnsName

