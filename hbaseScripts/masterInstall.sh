#!/bin/bash
#run this script on the hbase master to install the required software

apt-get install --yes nano python-pip git
pip install happybase requests isodate boto3
git clone https://github.com/JustinL42/Team42Project.git
