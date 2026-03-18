# hp-data-engineer

This branch contains response to HealthPartners Data Engineer technical interview assessment

## Running

To run, enter the directory where the data_extract.py script is stored and run these two commands 
```
pip install -r requirements.txt 
python data_extract.py [or python3 data_extract.py]
```

## Sample Data 
A few sample CSVs are found in the hospital_output folder. 

## Scheduling
This script can be run daily. It is designed to only update CSV files that have not been read at all, or have a modified date that is more recent than the last time they were read. Scheduling is not currently built in to the script but can be aded with cron
**example**
```
crontab -e 
0 6 * * * /path/to/python /path/to/data_extract.py
```

This will run the python script every day at 6am
