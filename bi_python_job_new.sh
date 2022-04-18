#!/bin/bash
echo "Running Python BI utility"
sudo systemctl restart postgresql.service
#sudo systemctl stop postgresql.service
#sudo systemctl start postgresql.service
#sudo systemctl enable postgresql.service
python3 /home/ubuntu/abhijit/BI_reporting_utility/run_main.py

echo "Job finished"
