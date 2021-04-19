### Date created
19-April-2021

### Project Title
DataLoader

### Description
Process uses sqlloader process to load multiple files into database.
Using Config File we can
Select Files with Specific PreFix from Input Directory using Parameters when running DataLoader File
Can load data into different Tables using ctrl files in ctrlFile Directory
Can select multiple files in multiple folders in Input Directory
Can Process files with Headers and Footers and load the data into Database

### Files used

DataLoader.sh
dataloader_config.csv
dataloader.env

### Installation




### Usage
Without Parameters
DataLoader.sh

With Parameter
DataLoader.sh ARG1

Sample Data in dataloader_config.csv
ArgValue,ListValue,HeaderSize,FooterSize,ctrlFile
ARG1,ev10,10,7,RBMctrlfile.ctrl
