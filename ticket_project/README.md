# Tickets Project Group

The primary purpose of this project is to examine the distribution of parking
tickets and transportation permits across the City of Chicago. In this project,
you will find tools to analyze theses questions together and in insolation. We
provide a matching and mapping system in which users an specify attributes of a
ticket or permit, find similar tickets/permits, and map their occurrence. These
resulting maps allow users to see where certain types of tickets/permits are
more common across the city or where matching tickets/permits have occurred
within a particular geographic area of the city. Additionally, the project links
together the dataset on permits and tickets, allowing users to see the
relationship between tickets and permits, including where permits are most
disruptive to parking patterns.

## Data Sources
Data in this project comes from four sources:
1. ProPublica's City of Chicago Parking Ticket Dataset (stored locally): 
https://www.propublica.org/datastore/dataset/chicago-parking-ticket-data
(nb, this dataset is 19.4GB in size and is filtered considerably for use in
this project)

2. City of Chicago Data Portal's Transportation Department Permits Dataset (API):
https://data.cityofchicago.org/Transportation/Transportation-Department-Permits/pubx-yq2d

3. City of Chicago Data Portal's Neighborhood Bounaries (API):
https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Neighborhoods/9wp7-iasj

4. City of Chicago Data Portal's Zip Code Boundaries (API): 
https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-ZIP-Codes/gdcf-axmw

## Requirements
The following Python packages are needed to run this software:

| Package | Version |
--------------------
| descartes | 1.1.0 |
| geocoder | 1.38.1 |
| geopandas | 0.4.1 |
| mapclassify | 2.0.1 |
| matplotlib | 3.0.2 |
| numpy | 1.16.2 |
| pandas | 0.23.4 |
| rtree | 0.8.3 |
| shapely | 1.6.4.post2 |
| sodapy | 1.5.2 |


Additionally, rtree requires libspatialindex library, avaliable at https://libspatialindex.org/.
Our software uses version 1.8.5.
## Running this software

### First things first
To avoid storing unnecessarily large CSV files in GitLab, a reduced version of
the parking tickets dataset should downloaded by navigating to the root of the
project folder and running the following command:
```
sh get_files.sh
```
This will download the necessary files from box along with a number of random
samples of the tickets dataset for testing purposes. 

As previously mentioned, the full parking tickets dataset is over 19GB in size,
which makes it difficult to process within a VM or the machines in CSIL (default
memory quota is 5GB) and difficult to share between computers. To address this,
we wrote a module called shrink_tickets.py that reduces the size of our dataset,
and ran this module on our personal machines. A sample of the entire 19GB
is provided in the data/raw/full_dataset_sample, so that the functionality of
this module can be tested on the VM. This module can be accessed with the
following command:
```
python3 shrink_tickets.py <path to full dataset (or sample)> <output dir>
```
The results will be a reduced size CSV file of parking tickets data and a CSV
mapping violation codes to violation descriptions.

The reduction in size is achieved by eliminating a number of columns that we
don't need for our analysis or that are duplicated and by filtering tickets
occuring before 7/13/2015. This date is selected because this is the date on
which the City of Chicago began using its current system for tracking
transportation department permits. While some permits from before this date are
available in the transportation department permits dataset, the quality of this
data is unclear.

### Running the main program