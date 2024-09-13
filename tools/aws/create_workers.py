import argparse
import subprocess


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--num_workers", type=int, default=1)
    parser.add_argument("--instance_type", type=str, default="c5.4xlarge")
    parser.add_argument("--subnet_id", type=str, default="")
    parser.add_argument("--placement_group_id", type=str, default="")
    args = parser.parse_args()
    subnet_id = args.subnet_id
    placement_group_id = args.placement_group_id
    print(placement_group_id)
    print(subnet_id)
    for i in range(args.num_workers):
        inline_json = '''
{
  "MaxCount": 1,
  "MinCount": 1,
  "ImageId": "ami-09efc42336106d2f2",
  "InstanceType": "c5.4xlarge",
  "NetworkInterfaces": [
    {
      "SubnetId": ''' f"\"{subnet_id}\"" ''',
      "DeviceIndex": 0
    }
  ],
  "MetadataOptions": {
    "HttpEndpoint": "enabled",
    "HttpPutResponseHopLimit": 2,
    "HttpTokens": "required"
  },
  "TagSpecifications": [
    {
      "ResourceType": "instance",
      "Tags": [
        {
          "Key": "Name",
          "Value": ''' f"\"Worker-{i}\"" '''
        },
        {
          "Key": "ClusterNodeType",
          "Value": "Worker"
        }
      ]
    }
  ],
  "Placement": {
    "GroupId": ''' f"\"{placement_group_id}\"" '''
  }
}
'''
        cmd = f"aws ec2 run-instances --cli-input-json '{inline_json}'"
        print(cmd)
        capture = subprocess.run(cmd, shell=True, capture_output=True)
        print(capture)
