# OBservator Server
Key codes for "OBservator" project on AWS EC2 Instance

## Server Structure (Tree)
```
/home/ubuntu/project
├── codes
│   ├── etc
│   └── main
├── data
│   ├── csv
│   │   ├── orderbooks
│   │   └── ticks
│   ├── key
│   ├── queue
│   └── raw
│       ├── orderbooks
│       └── ticks
└── logs
    ├── crontab.log
    └── date
```

## Crontab (Automation)
```
0 0 * * * /usr/bin/python3 /home/ubuntu/project/codes/etc/delete.py >> /home/ubuntu/project/logs/crontab.log 2>&1
```
```
1 0 * * * /home/ubuntu/anaconda3/envs/venv/bin/python /home/ubuntu/project/codes/etc/csv_convert.py >> /home/ubuntu/project/logs/crontab.log 2>&1
```
```
5 0 * * * /home/ubuntu/anaconda3/envs/venv/bin/python /home/ubuntu/project/codes/etc/s3_upload.py >> /home/ubuntu/project/logs/crontab.log 2>&1
```