# Himalayan Database Import Toll Into Neo4j Graph Database
This is a tool to import the data from the Himalayan Database into a Neo4j graph database.

![](docs/neo4j-import-tool-header.jpg)

## Getting Started
This repository uses [Poetry](https://python-poetry.org)to manage the Python dependencies and ensure that the correct 
versions are used. There are two ways to create a Python environment with the correct dependencies: 
* Using Anaconda to manage the Python environment and Poetry to manage the dependencies.
* Using Poetry for both the environment and the dependencies.

Once you have cloned the repository follow one of the two following methods to create the Python environment and
install the dependencies.
### Using Anaconda and Poetry
In your command line, from the repository folder follow the steps below:
1. Create a new Anaconda environment with the following command (this will install Poetry within the environment):
```
conda create --name hdb-neo4j-import --file assets/conda/conda-win-64.lock
```
2. Activate the new environment:
```
conda activate hdb-neo4j-import
```
3. Use Poetry to install the dependencies:
```
poetry install
```
### Using Poetry
To use Poetry for both the environment and the dependencies, you will need to install the proper version of Python and 
Poetry by yourself and instruct Poetry to use the correct Python version. When using Poetry to create a new environment,
it will automatically name the environment after the folder name. The virtual environment is created in the 
`{cache-dir}/virtualenvs`  or use the `{project-dir}/.venv` directory when one is available.

1. Install the latest version of Python 3.10 following the instructions on the 
[Python website](https://www.python.org/downloads/).
2. To install Poetry, follow the instructions on the [Poetry website](https://python-poetry.org/docs/#installation).
3. In your command line, from the repository folder, first disable any virtual environment:
```
poetry env use system
```
4. Use Poetry to create a new environment:
```
poetry env use /full/path/to/python
```
For example:
```
poetry env use D:\Python\Python310\python.exe
```
5. Use Poetry to install the dependencies:
```
poetry install
```
## How to use the tool?
## To Dos
- [ ] Add pytest tests for the Nepal Himal Peak Profile website scraper script
- [ ] Add pytest tests for the data processing scripts