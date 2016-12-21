#!/usr/bin/bash
echo "Running benchmark update `date`"
HOME=/home/ec2-user
BENCHMARK_REPO=$HOME/dask-benchmarks
DASK_DIR=$BENCHMARK_REPO/dask
DISTRIBUTED_DIR=$BENCHMARK_REPO/distributed

source activate dask-asv

echo "Updating benchmark repo..."
cd $BENCHMARK_REPO
git checkout master
git pull

echo "Running dask benchmarks..."
cd $DASK_DIR
asv --config $HOME/asv.dask.conf.json run NEW
DASK_STATUS=$?
if [ "$DASK_STATUS" -eq "0" ]; then
  echo "Generating dask html files..."
  asv --config $HOME/asv.dask.conf.json publish
fi

echo "Running distributed benchmarks..."
cd $DISTRIBUTED_DIR
asv --config $HOME/asv.distributed.conf.json run NEW
DISTRIBUTED_STATUS=$?
if [ "$DISTRIBUTED_STATUS" -eq "0" ]; then
  echo "Generating distributed html files..."
  # Currently install dask dependency for distributed via pip install git+http to
  # get current dask master. asv does not directly support this even though you
  # can get it to work. However directory structure gets messed up and machine.json
  # is not in the correct location to generate the graphs. Thus this hack to copy it
  # to the right locations before running publish.
  find /home/ec2-user/results/distributed/aws-ec2-c4.xlarge -type d -exec cp /home/ec2-user/results/distributed/aws-ec2-c4.xlarge/machine.json {} \;
  asv --config $HOME/asv.distributed.conf.json publish
fi

STATUSES=$(($DASK_STATUS + $DISTRIBUTED_STATUS))
if [ "$STATUSES" -lt "2" ]; then
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
