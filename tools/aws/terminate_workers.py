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
   instance_ids = get_instance_ids()
   if len(instance_ids) != 0:
         instance_id_strings = " ".join([f"\"{instance_id}\"" for instance_id in instance_ids.values()])
         cmd = f"aws ec2 terminate-instances --instance-ids {instance_id_strings}"
         print(cmd)
         capture = subprocess.run(cmd, shell=True, capture_output=True)
         cmd = f"aws wait instance-terminated --instance-ids {instance_id_strings}"
         print(cmd)
         capture = subprocess.run(cmd, shell=True, capture_output=True)