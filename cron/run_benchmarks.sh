#!/usr/bin/bash
echo "Running benchmark update `date`"
BENCHMARK_REPO=${BENCHMARK_REPOSITORY:-$HOME/dask-benchmarks}
DASK_DIR=$BENCHMARK_REPO/dask
DISTRIBUTED_DIR=$BENCHMARK_REPO/distributed
DASK_CONFIG=${DASK_ASV_CONFIG:-$HOME/asv.dask.conf.json}
DISTRIBUTED_CONFIG=${DISTRIBUTED_ASV_CONFIG:-$HOME/asv.distributed.conf.json}

echo "Creating conda environment..."
conda create -n dask-asv python=3.5
pip install asv
source activate dask-asv

echo "Updating benchmark repo..."
cd $BENCHMARK_REPO
git checkout master
git pull

echo "Running dask benchmarks..."
cd $DASK_DIR
echo "    Benchmarking new commits..."
asv --config $DASK_CONFIG run NEW
DASK_STATUS=$?
echo "    Running new benchmarks on existing commits..."
asv --config $DASK_CONFIG run EXISTING --skip-existing-successful
DASK_STATUS=$(($DASK_STATUS + $?))
if [ "$DASK_STATUS" -lt "2" ]; then
  echo "Generating dask html files..."
  asv --config $DASK_CONFIG publish
fi

echo "Running distributed benchmarks..."
cd $DISTRIBUTED_DIR
echo "    Benchmarking new commits..."
asv --config $DISTRIBUTED_CONFIG run NEW
DISTRIBUTED_STATUS=$?
echo "    Running new benchmarks on existing commits..."
asv --config $DISTRIBUTED_CONFIG run EXISTING --skip-existing-successful
STATUS=$(($DISTRIBUTED_STATUS + $?))
if [ "$DISTRIBUTED_STATUS" -lt "2" ]; then
  echo "Generating distributed html files..."
  # Currently install dask dependency for distributed via pip install git+http to
  # get current dask master. asv does not directly support this even though you
  # can get it to work. However directory structure gets messed up and machine.json
  # is not in the correct location to generate the graphs. Thus this hack to copy it
  # to the right locations before running publish.
  find /home/ec2-user/results/distributed/aws-ec2-c4.xlarge -type d -exec cp /home/ec2-user/results/distributed/aws-ec2-c4.xlarge/machine.json {} \;
  asv --config $DISTRIBUTED_CONFIG publish
fi

# exit on error otherwise it might still commit
set -e

STATUS=$(($DASK_STATUS + $DISTRIBUTED_STATUS))
if [ "$STATUS" -lt "4" ]; then
  echo "Publishing results to github..."
  cd $BENCHMARK_REPO
  git checkout gh-pages
  cp -r $HOME/html .
  rm -rf results
  mv html results
  git add results
  git commit -am "Auto-committed by benchmark script" 
  git push
else
  echo "No updates to publish..."
fi
