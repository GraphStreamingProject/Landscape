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
    args = parser.parse_args()

    instance_ids = get_instance_ids()
    stop_instance_ids = {k: v for k, v in instance_ids.items() if k > args.num_workers}
    start_instance_ids = {k: v for k, v in instance_ids.items() if k <= args.num_workers}
    start_instance_id_strings = " ".join([f"\"{instance_id}\"" for instance_id in start_instance_ids.values()])
    stop_instance_ids_strings = " ".join([f"\"{instance_id}\"" for instance_id in stop_instance_ids.values()])
    cmd = f"aws ec2 start-instances --instance-ids {start_instance_id_strings}"
    print(cmd)
    capture = subprocess.run(cmd, shell=True, capture_output=True)

    cmd = f"aws ec2 stop-instances --instance-ids {stop_instance_ids_strings}"
    print(cmd)
    capture = subprocess.run(cmd, shell=True, capture_output=True)

    if start_instance_id_strings != "":
        cmd = f"aws ec2 wait instance-running --instance-ids {start_instance_id_strings}"
        print(cmd)
        capture = subprocess.run(cmd, shell=True, capture_output=True)

    # if stop_instance_ids_strings != "":
        # cmd = f"aws ec2 wait instance-stopped --instance-ids {stop_instance_ids_strings}"
        # print(cmd)
        # capture = subprocess.run(cmd, shell=True, capture_output=True)