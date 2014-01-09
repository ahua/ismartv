
cd /home/deploy/work/Tools/plus_start_exit/p
cp /home/deploy/work/Tools/splunk3/var/run/splunk/k_exit.csv plus/
python run.py plus/k_exit.csv plus/k_plus_start_exit.txt
python splunk.py localhost 11116 plus/k_plus_start_exit.txt
rm plus/k_exit.csv plus/k_plus_start_exit.txt -f
rm /home/deploy/work/Tools/splunk3/var/run/splunk/k_exit.csv -f

cp /home/deploy/work/Tools/splunk3/var/run/splunk/s_exit.csv plus/
python run.py plus/s_exit.csv plus/s_plus_start_exit.txt
python splunk.py localhost 21116 plus/s_plus_start_exit.txt
rm plus/s_exit.csv plus/s_plus_start_exit.txt -f
rm /home/deploy/work/Tools/splunk3/var/run/splunk/s_exit.csv -f

