
if [[ $# -ne 3 ]]; then
  echo "Invalid arguments. Require expr_type, result_file, region"
  echo "expr_type:    Either 'full' or 'limited'. How many data points to collect."
  echo "result_file:  CSV for results"
  echo "region:       Region where the nodes be"
  exit
fi

expr_type=$1
result_file=$2
region=$3

# CUBESKETCH + STANDALONE
cd ../build
cmake -DUSE_CUBE:BOOL=ON -DUSE_STANDALONE:BOOL=ON ..
make -j
cd -

echo "===========  ABLATIVE EXPERIMENTS 1  ==========="

if [ $expr_type == 'full' ]; then
  python3 aws/run_first_n_workers.py --num_workers 1
  yes | bash setup_tagged_workers.sh $region 36 8
  bash scale_experiment.sh $result_file 1 1 1 1 ', Cube + Standalone'
else
  python3 aws/run_first_n_workers.py --num_workers 2
  yes | bash setup_tagged_workers.sh $region 36 8
  bash scale_experiment.sh $result_file 2 2 1 1 ', Cube + Standalone'
fi

python3 aws/run_first_n_workers.py --num_workers 16
yes | bash setup_tagged_workers.sh $region 36 8
bash scale_experiment.sh $result_file 16 16 1 1 ', Cube + Standalone'

python3 aws/run_first_n_workers.py --num_workers 32
yes | bash setup_tagged_workers.sh $region 36 8
bash scale_experiment.sh $result_file 32 32 1 3 ', Cube + Standalone'

python3 aws/run_first_n_workers.py --num_workers 48
yes | bash setup_tagged_workers.sh $region 36 8
bash scale_experiment.sh $result_file 48 48 1 3 ', Cube + Standalone'


# CAMEOSKETCH + STANDALONE
cd ../build
cmake -DUSE_CUBE:BOOL=OFF -DUSE_STANDALONE:BOOL=ON ..
make -j
cd -

echo "===========  ABLATIVE EXPERIMENTS 2  ==========="

if [ $expr_type == 'full' ]; then
  python3 aws/run_first_n_workers.py --num_workers 1
  yes | bash setup_tagged_workers.sh $region 36 8
  bash scale_experiment.sh $result_file 1 1 1 1 ', Cameo + Standalone'
else
  python3 aws/run_first_n_workers.py --num_workers 2
  yes | bash setup_tagged_workers.sh $region 36 8
  bash scale_experiment.sh $result_file 2 2 1 1 ', Cameo + Standalone'
fi

python3 aws/run_first_n_workers.py --num_workers 16
yes | bash setup_tagged_workers.sh $region 36 8
bash scale_experiment.sh $result_file 16 16 1 3 ', Cameo + Standalone'

python3 aws/run_first_n_workers.py --num_workers 32
yes | bash setup_tagged_workers.sh $region 36 8
bash scale_experiment.sh $result_file 32 32 1 5 ', Cameo + Standalone'

python3 aws/run_first_n_workers.py --num_workers 48
yes | bash setup_tagged_workers.sh $region 36 8
bash scale_experiment.sh $result_file 48 48 1 5 ', Cameo + Standalone'

# Restore default settings
cd ../build
cmake -DUSE_CUBE:BOOL=OFF -DUSE_STANDALONE:BOOL=OFF ..
make -j
cd -
