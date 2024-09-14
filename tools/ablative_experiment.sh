
region=$1

# CUBESKETCH + STANDALONE
cd ../build
cmake -DUSE_CUBE:BOOL=ON -DUSE_STANDALONE:BOOL=ON ..
make -j
cd -

echo "===========  ABLATIVE EXPERIMENTS 1  ==========="

python3 aws/run_first_n_workers.py --num_workers 1
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 1 1 1 1 'Cube + Standalone'

python3 aws/run_first_n_workers.py --num_workers 16
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 16 16 1 3 'Cube + Standalone'

python3 aws/run_first_n_workers.py --num_workers 32
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 32 32 1 7 'Cube + Standalone'

python3 aws/run_first_n_workers.py --num_workers 48
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 48 48 1 11 'Cube + Standalone'


# CAMEOSKETCH + STANDALONE
cd ../build
cmake -DUSE_CUBE:BOOL=OFF -DUSE_STANDALONE:BOOL=ON ..
make -j
cd -

echo "===========  ABLATIVE EXPERIMENTS 2  ==========="

python3 aws/run_first_n_workers.py --num_workers 1
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 1 1 1 1 'Cameo + Standalone'

python3 aws/run_first_n_workers.py --num_workers 16
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 16 16 1 3 'Cameo + Standalone'

python3 aws/run_first_n_workers.py --num_workers 32
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 32 32 1 7 'Cameo + Standalone'

python3 aws/run_first_n_workers.py --num_workers 48
yes | bash setup_tagged_workers $region 36 8
bash scale_experiment 48 48 1 11 'Cameo + Standalone'

# Restore default settings
cd ../build
cmake -DUSE_CUBE:BOOL=OFF -DUSE_STANDALONE:BOOL=OFF ..
make -j
cd -
