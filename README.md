# algopy

- copy config-sample.toml to config.toml and enter your details

- in the config.toml file you have to give data_account_name and trading_account_name, both can be same or different if the order has to go to one broker and data comes from another broker


- server setup

```
# change the timezone and restart server

sudo timedatectl set-timezone Asia/Kolkata
sudo reboot

```  

- install requirements
```sh
# create a virtual environment

python3 -m venv env

source ./env/bin/activate

pip install -r requirements.txt

```



- run from shell
```sh

# activate virtual environment if not activate already
source env/bin/activate

python3 src/main.py

```


- schedule a cron job
```
# change the path as per your machine

15 9 * * MON,TUE,WED,THU,FRI cd /home/user/algopy && /home/user/algopy/env/bin/python3 -u src/main.py >> output.txt 2>&1

```

