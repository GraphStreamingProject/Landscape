i=`ec2-metadata -i | awk '{print $NF}'`
aws ec2 create-tags --tag Key=ClusterNodeType,Value=Master --resources $i
