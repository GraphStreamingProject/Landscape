
i=`ec2-metadata -i | awk '{print $NF}'`
z=`ec2-metadata -z | awk '{print $NF}'`

echo $i $z
