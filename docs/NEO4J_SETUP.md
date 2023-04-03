# Neo4j Setup
This project uses a local instance of a Neo4j database. Before starting this setup we encourage you to have [Anaconda](https://www.anaconda.com/products/distribution) and run this in a separate environment for easy setup and installation.

## Getting started
1. Clone our [project repo](https://dagshub.com/sjtalkar/capstone_himalayas)
2. Via Anaconda create a new environment called "neo4j"
```
conda create --name neo4j
```
3. Open your command line and activate the environment. With anaconda as part of your machine PATH you can do this with
```
conda activate neo4j
```
4. Navigate the folder where you cloned the repo. The "requirements.txt" should be in the directory are now in.
```
cd /whereyoustoredtherepo/capstone_himalays/
```
5. Install the package requirements
```
pip install -r requirements.txt
```

Once the requirements are installed proceed with the following instructions.

# 

You need to follow all the instructions below to be able to run the project and reproduce the results.
1. Download and install Neo4j desktop
2. Install the Neo4j  `APOC` and `Graph Data Science Library` plugins
2. Follow all the [configuration](#configuration) steps below
3. [Import the Himalayan Database into Neo4j](#import-the-himalayan-database-into-neo4j)
4. [Copy the Pre-Computed Graph Data](#copy-the-pre-computed-graph-data)
## Versions
This project uses:
* `Neo4j Desktop` 1.5.7
* `Neo4j DBMS` in version 5.4.0 
* `APOC` Plugin in version 5.4.1
* `Graph Data Science Library` Plugin in version 2.3.1
## Download and Installation
Download the Neo4j Desktop application from [here](https://neo4j.com/download-neo4j-now/). You will need to register
with your email address and create a password (note the password down, you will need it later). Once you have done this 
you can download the application and you will receive a software key. You will need this key to activate the application
following the instructions 
[here](https://neo4j.com/developer/kb/how-to-use-activation-keys/).
## Configuration
### Neo4j Desktop Setup
Once you have installed the Neo4j Desktop application, you will need to create a new local DBMS instance and 
install the aforementioned plugins. You can follow the instructions in the following screenshot:
1. Create a new Project
2. Add a new local Neo4j DBMS instance to your project
3. Update the DBMS memory configuration as described below (you might have to come back and adjust these settings later
depending on your environment, if you run into memory issues). See the 
[Neo4j Memory Configuration](#neo4j-memory-configuration) section below for more details.
4. Install the `APOC` and `Graph Data Science Library` plugins
5. Set version to 5.4.0 (desktop installs as 5.3.0)

![](https://dagshub.com/sjtalkar/capstone_himalayas/raw/main/docs/neo4j/neo4j-desktop-setup-1.jpg)
### Neo4j Memory Configuration
Importing the entire dataset and running the graph algorithms requires a lot of memory. In our environment, we had to configure the Neo4j DBMS as
follows to avoid memory issues:
```bash
dbms.memory.heap.initial_size=2.5G
dbms.memory.heap.max_size=4G
dbms.memory.pagecache.size=2.5G
```
To update the memory configuration in the Neo4j desktop, 
1. Open you project 
2. Select your DBMS instance 
3. Click on the [...] button, right of the `Start` and `Open` buttons and click on `Settings...` (see point (3) on the screenshot above)
4. Scroll down to the bottom of the file and edit the memory settings as described above
5. And click on the `Apply` button. If your DBMS instance is running, you will need to restart it.

![](https://dagshub.com/sjtalkar/capstone_himalayas/raw/main/docs/neo4j/neo4j-memory-config.jpg.jpg)

### Neo4j Plugins: APOC and Graph Data Science Library
1. Click on Graph DBMS 
2. Select "Plugins" from right side menu
3. Choose plugins and select "Install"

### Neo4j APOC Plugin Configuration
#### Enabling Procedures
After installing the `APOC` and `Graph Data Science Library` plugins, makes sure the plugins procedures are authorized. 
To do so go back in the DMS settings (see above), starting from the bottom of the configuration, scroll back up and 
look for the below setting and make sure it is not commented out. This will appear in the section `Miscellaneous configuration`.
```
dbms.security.procedures.unrestricted=apoc.*,gds.*
```

### Neo4j Upgrade
1. Select the "Upgrade" tab next to "Plugins"
2. Choose "5.4.0" from the drop down and click "upgrade"

#### Enabling File Import and Export
In the Neo4j desktop,
1. Open you project
2. Select your DBMS instance
3. Click on the [...] button, right of the `Start` and `Open` buttons and click on `Settings...` 
4. Click on `Open folder` then `DBMS`
![](https://dagshub.com/sjtalkar/capstone_himalayas/raw/main/docs/neo4j/neo4j-open-dbms-folder.jpg)
5. This will open the Neo4j DBMS folder in your file explorer. Navigate into the `conf` subfolder.
6. In that folder, create a file name `apoc.conf` and add the following lines:
```bash
apoc.export.file.enabled=true
apoc.import.file.enabled=true
```
7. If your DBMS instance is running, you will need to restart it.
### Python Environment Parameters
Create an  `\.env` file in the root folder of the git repository that was cloned, you must specify the following parameters:
```bash
NEO4J_SERVER_URL=<the url of the local Neo4j DBMS instance. Typically: neo4j://localhost:7687>
NEO4J_DATABASE_NAME=<the name of the database to use. If not set will default to "himalayas">
NEO4J_SERVER_USERNAME=<your Neo4j user name. Typically: neo4j>
NEO4J_SERVER_PASSWORD=<your password>
```
Example:
```bash
NEO4J_SERVER_URL=neo4j://localhost:7687
NEO4J_SERVER_USERNAME=neo4j
NEO4J_SERVER_PASSWORD=MySuperSecretNeo4jP@ssword
```
## Import the Himalayan Database into Neo4j
Once the .env file is created in the Neo4j project root directory, start the database in Neo4j desktop and open the terminal (select the three dots to pull up menu and select "terminal").

1. Activate the neo4j environment
2. Navigate to the capstone_himalaya repo within the terminal
3. Execute the following command:
```bash
python lib\neo4j_import\neo4j_import.py
```
## Copy the Pre-Computed Graph Data
Because some graph algorithm are stochastic and their results cannot be made reproducible through deterministic seeding,
and some algorithms (e.g. CELF influence maximisation) can take hours to run, we have pre-computed the graph results and 
exported them for you. If you don't use the pre-computed results, you will get slightly different results and the
notebooks will automatically bypass the long running algorithms.

Unfortunately, due to a bug in the Neo4j APOC procedures we copy import and export the pre-computed graph data directly 
from the project's repository `data\neo4j_results` folder. We can only import/export from the Neo4j DBMS default 
`import` folder.
1. If you haven't done already fallow the instructions above in the 
[Enabling File Import and Export](#enabling-file-import-and-export) section.
2. From the Neo4j desktop, open you project
3. Select your DBMS instance
4. Click on the [...] button, right of the `Start` and `Open` buttons and click on `Settings...`
5. Click on `Open folder` then `Import`
6. This will open the Neo4j DBMS `import` folder in your file explorer.
7. Copy-paste all the files from the repository's `data\neo4j_results` folder into the Neo4j DBMS `import` folder.
## Neo4j Bloom
Once you have imported the data into the Neo4j database, you can use the Neo4j Desktop to run Cypher queries and
visualize the data. However, by default, the Neo4j Desktop application only displays 500 nodes in the graph view and
trying to go beyond that will result in extremely slow performance. 

Neo4j offers several additional applications to visualize the data. The list of available applications can be found
[here](https://install.graphapp.io/). To install any of these applications, with your Neo4j Desktop application
running, just click on any of the `Install` buttons in the list of applications on that page.

We recommend to use the `Neo4j Bloom` application as an alternative to the Neo4j Desktop application for basic
querying and visualization of the data. Please refer to the 
[Neo4j Bloom documentation](https://neo4j.com/developer/neo4j-bloom/) for more information on how use the application.
