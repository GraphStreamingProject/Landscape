

# CUBESKETCH + STANDALONE
cd ../build
cmake -DUSE_CUBE:BOOL=ON -DUSE_STANDALONE:BOOL=ON ..
make -j
cd -

echo "===========  ABLATIVE EXPERIMENTS 1  ==========="

# TODO: ENSURE ONLY 1 MACHINE ON
bash scale_experiment 1 1 1 1

# TODO: TURN ON 15 MACHINE
bash scale_experiment 16 16 1 3

# TODO: TURN ON 16 MACHINE
bash scale_experiment 32 32 1 7

# TODO: TURN ON 32 MACHINE
bash scale_experiment 64 64 1 11


# CAMEOSKETCH + STANDALONE
cd ../build
cmake -DUSE_CUBE:BOOL=OFF -DUSE_STANDALONE:BOOL=ON ..
make -j
cd -

echo "===========  ABLATIVE EXPERIMENTS 2  ==========="

# TODO: ENSURE ONLY 1 MACHINE ON
bash scale_experiment 1 1 1 1

# TODO: TURN ON 15 MACHINE
bash scale_experiment 16 16 1 3

# TODO: TURN ON 16 MACHINE
bash scale_experiment 32 32 1 7

# TODO: TURN ON 32 MACHINE
bash scale_experiment 64 64 1 11

# Restore default settings
cd ../build
cmake -DUSE_CUBE:BOOL=OFF -DUSE_STANDALONE:BOOL=OFF ..
make -j
cd -
