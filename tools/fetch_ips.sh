#!/bin/bash
echo `aws ec2 describe-instances --region us-east-1 --no-paginate --filters Name=instance-state-name,Values=running --filters Name=tag:ClusterNodeType,Values=Master | jq -r .Reservations[0].Instances[].NetworkInterfaces[0].PrivateDnsName`
echo `aws ec2 describe-instances --region us-east-1 --no-paginate --filters Name=instance-state-name,Values=running --filters Name=tag:ClusterNodeType,Values=Worker | jq -r .Reservations[0].Instances[].NetworkInterfaces[0].PrivateDnsName`

