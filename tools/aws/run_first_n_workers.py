import argparse
import subprocess
import json


def get_instance_ids():
  instances_query_cmd = "aws ec2 describe-instances --output json"
  capture = subprocess.run(instances_query_cmd, shell=True, capture_output=True)
  instances = json.loads(capture.stdout)['Reservations'][0]['Instances']
  instance_ids = {}
  for instance in instances:
      # instance_ids[instance['Tags']['Value']] = instance['InstanceId']
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
    args = parser.parse_args()
    subnet_id = args.subnet_id
    placement_group_id = args.placement_group_id

    instance_ids = get_instance_ids()

    stop_instance_ids = {k: v for k, v in instance_ids.items() if k > args.num_workers}
    start_instance_ids = {k: v for k, v in instance_ids.items() if k <= args.num_workers}

    start_instance_id_strings = " ".join([f"\"{instance_id}\"" for instance_id in start_instance_ids.values()])
    stop_instance_ids_strings = " ".join([f"\"{instance_id}\"" for instance_id in stop_instance_ids.values()])
    cmd = f"aws ec2 sstart-instances f{start_instance_id_strings}"
    capture = subprocess.run(cmd, shell=True, capture_output=True)
    cmd = f"aws ec2 stop-instances f{start_instance_id_strings}"
    capture = subprocess.run(cmd, shell=True, capture_output=True)
      # TODO - see if we need to use the capture. The answer is probably not.
print(f"Number running: {ctr}")