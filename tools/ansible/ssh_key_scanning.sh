input_file=$1

while read line; do
  ssh-keyscan $line >> ~/.ssh/known_hosts # add other machine's pub key to our known_hosts
done <$input_file
