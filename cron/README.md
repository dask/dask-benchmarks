# Automating Benchmarking

This directory contains a bash script and asv configuration files for automating benchmarking for dask and dask-distributed. The script takes advantage of an asv feature that allows it to benchmark all commits since the last benchmark. If no new commits were found for both repositories, the script will just exit. If commits were found for at least one, it will benchmark, generate html files, and publish to the gh-pages branch. 

Note that asv does have a built in feature for doing all the steps to publish to gh-pages. This was not used as we are benchmarking two packages and to achieve a custom url structure.

Also note that distributed has a dependency on dask. The master branch for distributed has a strong dependency on dask master. This requires pip installing the dask dependency from github. As this is not directly supported by asv, a small hack is required as commented in the script.

## Setting up new machine (for CentOS, adapt as needed)

Install requirements:

```
sudo yum update
sudo yum upgrade
sudo yum install wget git gcc gcc-c++ bzip2
git clone git@github.com:dask/dask-benchmarks.git
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh  # install to default location, yes to append path in bashrc
source ~/.bashrc
```

Generate key and add to your github ssh keys:

```
ssh-keygen -t rsa -b 4096
cat .ssh/id_rsa.pub
```

Set git username and email

```
git config --global user.name "My Name"
git config --global user.email "me@email.com"
```

## Configuring benchmark script

The script pulls down the latest dask-benchmarks from the repository. If you want to autodeploy changes to the asv config files and the script, you can run the script directly from the cloned repository. Otherwise, copy the files to another location and configure cron accordingly.

The script needs to know the location of the config files and the benchmark clone. It defaults to the common directory structure for an AWS EC2 instance but you can override by setting environment variables for `BENCHMARK_REPO`, `DASK_ASV_CONFIG`, `DISTRIBUTED_ASV_CONFIG`.

## Configuring cron

Run `crontab -e` and add the following line:

```
0 12 * * * /path/to/run_benchmarks.sh > /path/to/benchmarking.log 2>&1
```

