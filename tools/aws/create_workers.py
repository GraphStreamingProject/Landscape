import argparse
import subprocess
import json

def get_instance_ids():
  instances_query_cmd = "aws ec2 describe-instances --output json"
  capture = subprocess.run(instances_query_cmd, shell=True, capture_output=True)
  instances = json.loads(capture.stdout)['Reservations']
  instances = [instance['Instances'][0] for instance in instances]
  instance_ids = {}
  for instance in instances:
      if instance['State']['Name'] == 'terminated':
         continue
      for tags in instance['Tags']:
        if tags.get('Key') == 'Name':
          if tags.get('Value').split('-')[0] != 'Worker':
            continue
          id_number = tags.get('Value').split('-')[1]
          try:
            id_number = int(id_number)
          except:
              pass
          instance_ids[id_number] = instance['InstanceId']
  return instance_ids

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--num_workers", type=int, default=1)
    parser.add_argument("--instance_type", type=str, default="c5.4xlarge")
    parser.add_argument("--subnet_id", type=str, default="")
    parser.add_argument("--placement_group_id", type=str, default="")
    parser.add_argument("--security_groups", type=str, nargs='+', default=[])
    args = parser.parse_args()
    subnet_id = args.subnet_id
    placement_group_id = args.placement_group_id

    security_groups_json = json.dumps(args.security_groups)

    instance_ids = get_instance_ids()
    ctr = 0

    for i in range(1, args.num_workers+1):
      if i in instance_ids:
        continue
      ctr += 1
      inline_json = '''
{
  "MaxCount": 1,
  "MinCount": 1,
  "ImageId": "ami-0f3769c8d8429942f",
  "InstanceType": "c5.4xlarge",
  "KeyName": "Creation Key",
  "NetworkInterfaces": [
    {
      "SubnetId": ''' f"\"{subnet_id}\"" ''',
      "DeviceIndex": 0,
      "Groups": ''' f"{security_groups_json}" '''
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
      capture = subprocess.run(cmd, shell=True, capture_output=True)
      # TODO - see if we need to use the capture. The answer is probably not.
print(f"New Launched: {ctr}")