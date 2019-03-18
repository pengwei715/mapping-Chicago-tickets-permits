# Tickets Project Group

$503,113,540 in tickets. 140,516 Chicago Transportation Department permits.

The primary purpose of this project is to examine the distribution of parking
tickets and transportation permits across the City of Chicago. In this project,
you will find tools to analyze these questions together and in isolation. We
provide a matching and mapping system in which users a specify attributes of a
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

3. City of Chicago Data Portal's Neighborhood Boundaries (API):
https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Neighborhoods/9wp7-iasj

4. City of Chicago Data Portal's Zip Code Boundaries (API): 
https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-ZIP-Codes/gdcf-axmw

### A note on data coverage
Currently, parking tickets data covers from 7/13/2015 to 5/14/2018. The lower
limit is imposed by the developers as permits data is only consistently
available in the City of Chicago Data Portal Transportation Department Permits
dataset after this date (discussed more in the above section). The upper limit is
imposed by the dataset downloaded from ProPublica.

Transportation Departments data is up-to-date with the City of Chicago Data
Portal. Technically, there is no lower or upper limit on the dates it covers,
but for consistent coverage, we suggest only querying for dates after
7/13/2015.

Understanding which tickets are linked with permits turns out to be quite
difficult. A naive attempt to link any ticket with any permits that overlap
in location and time yields a number of tickets unrelated to parking, such as
having expired or missing license plates. Since our project was specifically
interested in how permits limit parking and result in tickets as a result of
limiting parking, we ended up making applying a number of assumptions (made
a priori and in consultation with the data) to hopefully get an accurate number
of tickets directly linked with permits limiting parking spaces. In particular,
when linking the two datasets, we limited to tickets whose violation is either
"PARKING/STANDING PROHIBITED ANYTIME" or "NO STANDING/PARKING TIME RESTRICTED",
and we limited the permits data to permits allowing for a full closure of the
street. This is an extremely conservative matching system, and more study on the
linkages of the two might provide a better rationale for how link permits and
tickets.

Finally, its worth noting how permits are plotted. The city provides latitude
and longitude coordinates for each permit, and we choose to use this to plot the
permits on a map. This system works pretty well for most permits, which cover a
small geographic area, but does not represent more expasive permits as well.

## Requirements
The following Python packages are needed to run this software:

| Package | Version |
|  ---- |  ---- |
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


Additionally, rtree requires libspatialindex library, available at 
https://libspatialindex.org/. Our software uses version 1.8.5.

## Running this software

### First things first
To avoid storing unnecessarily large CSV files in GitLab, a reduced version of
the parking tickets dataset should be downloaded by navigating to the root of the
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
is provided in the data/sample/full_dataset_sample.csv, so that the 
functionality of this module can be tested on the VM. This module can be
accessed from the terminal with the following command:
```
python3 shrink_tickets.py <path to full dataset (or sample)> <output dir>
```
The results will be a reduced size CSV file of parking tickets data and a CSV
mapping violation codes to violation descriptions. Be careful not to set the
output directory such that it will overwrite any dataset downloaded earlier.

The reduction in size is achieved by eliminating a number of columns that we
don't need for our analysis or that are duplicated and by filtering tickets
occurring before 7/13/2015. This date is selected because this is the date on
which the City of Chicago began using its current system for tracking
transportation department permits. While some permits from before this date are
available in the transportation department permits dataset, the quality of this
data is unclear.

The creation of CSV mapping violation codes to violation descriptions reflects
the fact that the current dataset stores the reason a ticket was issued in
two columns, violation_code and violation_description. To cut down on the size
of our CSV file containing the ticket dataset, we remove the
violation_descrption column since it is stored as strings and keep the
substantially smaller violation_description column. Since our the
violation_description column is more understandable to humans, however, we
create the CSV linking violation codes to violation descriptions so that we can
reassociate tickets with the violation descriptions as a categorical column
after reading into pandas.

The way the dataset is shrunk is also intentionally designed to make it easier to
structure the dataset in ways that make it easier to manipulate in pandas later.
For example, with over 6 million rows remaining after our date filter, storing
the address where each ticket occurred as a string would be quite expensive
memory-wise and could make computations in pandas difficult or impossible
depending on RAM size. So instead, we break address into three columns: street
number, street direction, and street name. When later reading this dataset into
Python, we can then store the street number as an unsigned integer, and the
street direction and street names as categoricals, which results in a
substantial decrease in deep memory usage when loading the dataset into pandas
from a CSV.

### Running the main program
The main program can be run from the command line using the following syntax:
```
python3 analyze  <dataset to use>  <dictionary-like JSON string specifying parameters to limit the dataset on>
```

Valid choices of dataset are:
- "tickets": the dataset of parking tickets
- "permits": the dataset of permits
- "linked": the dataset linking parking tickets to permits

Valid parameters for each data set are as follows:
- tickets
 - "violation": the reason for which a ticket was issued;  full list of valid
 entries is avaliable in inputs/violation_types.txt.
 - "start_date": a lower limit (inclusive) on the date a ticket was issued; must
 be of the form "YYYY-MM-DD".
 - "end_date": an upper limit (exclusive) on the date a ticket was issued; must
 be of the form "YYYY-MM-DD".
 - "location": a location to find tickets within a four square mile box of. For
 example, "1307 E. 60th Street, Chicago, IL"
 - "neighborhood": the name of a Chicago neighborhood. A full list of valid
 entries is available in inputs/neighborhood_names.txt.
- permits
 - "worktype": the reason for a ticket. A full list of valid entries is
 available in inputs/work_types.txt.
 - "start_date": a lower limit (inclusive) on the date a ticket was issued. Must
 be of the form "YYYY-MM-DD".
 - "end_date": an upper limit (exclusive) on the date a ticket was issued. Must
 be of the form "YYYY-MM-DD".
 - "location": a location to find tickets within a four square mile box of. For
 example, "1307 E. 60th Street, Chicago, IL"
 - 'closing_type': the type of street closure associated with the location.
 Valid entries are: "Curblane", "Sidewalk", "Full", "Partial", and "Intermitte"
- linked
 - "start_date": a lower limit (inclusive) on the date a ticket was issued; must
 be of the form "YYYY-MM-DD".
 - "end_date": an upper limit (exclusive) on the date a ticket was issued; must
 be of the form "YYYY-MM-DD".
 - "location": a location to find tickets within a four square mile box of. For
 example, "1307 E. 60th Street, Chicago, IL"
 - "neighborhood": the name of a Chicago neighborhood. A full list of valid
 entries is available in inputs/neighborhood_names.txt.

For example, to query parking tickets in Hyde Park during June 2017, one would
run the following command:
```
python3 analyze.py tickets '{"neighborhood": "Hyde Park", "start_date": "06-01-2017", "end_date": "07-01-2017"}'
```

To query all permits issued near 1307 E. 60th Street, Chicago, IL 60637 for
athletic events, one would run the following command:
```
python3 analyze.py permits '{"location": "1307 E. 60th Street, Chicago, IL 60637", "worktype": "Athletic"}'
```

To query all the parking tickets associated with Transportation Department
Permits in Woodlawn, one would run the following command:
```
python3 analyze.py linked '{"neighborhood": "Woodlawn"}'
```