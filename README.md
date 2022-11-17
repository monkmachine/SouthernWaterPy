# SouthernWaterPy

## Pre-Requisites
* Python must be installed (https://www.python.org/ftp/python/3.11.0/python-3.11.0-amd64.exe)  
* Chrome must be installed

## Installation
* Uzip the Zip file to a location on your hard drive  
* Navigate to the unzipped location using CMD  
* Run the following command  
```
pip install -r requirements.txt
```
This will install the required python modules
The run the following command to execute the python script
```
python SoutherWater.py
```
Chrome will open, it will hang for a few seconds whilst it waits for the page to load then will start looping through the spills tables. 
It can take some time to run but you can watch chrome clicking through the pages. Once it completes looping through the pages it will start
creating a csv file (formatted as DateTime_Spills.csv).  
  
  Once the CSV has been created chrome will close and your CSV will be created with the
spills data.
