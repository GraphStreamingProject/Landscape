#!/usr/bin/env bash

function runcmd {
  echo "Running command $@"
  "$@"
}

get_file_path() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

project_dir=$(get_file_path .)

echo "=== Landscape Experiments ==="
echo "This script runs the experiments for our paper Landscape. It is designed for Amazon EC2."
echo "It utilizes sudo commands to install packages and configure the system."
echo ""
while :
do
  read -r -p "Do you want to continue(Y/N): " c
  case "$c" in
    'N'|'n') exit;;
    'Y'|'y') break;;
  esac
done

echo "Installing tmux"
runcmd sudo yum update -y
runcmd sudo yum install -y tmux

runcmd mkdir build
echo "===== Cluster Status Summary =====" > build/cluster_status.txt
echo "Cluster status info will appear here once the experiments begin" >> build/cluster_status.txt

echo "Launching a tmux window for rest of code"
runcmd tmux new-session -d -s landscape-expr

runcmd tmux send -t landscape-expr:0 'bash tools/run_experiments.sh' ENTER
runcmd tmux pipe-pane -t landscape-expr:0 "cat>$project_dir/expr_log.txt"
runcmd tmux split-window -t landscape-expr:0
runcmd tmux send -t landscape-expr:0 'watch -n 1 cat build/cluster_status.txt' ENTER
runcmd tmux select-pane -t landscape-expr:0.0
runcmd tmux attach -t landscape-expr

