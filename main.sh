pkill -f main.py
pkill -f server.py
source venv/bin/activate
python main.py > sarah.log &
nohup python webserver/server.py > webserver.log &
