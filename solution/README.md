## DWH Challenge

### Prerequisites
* python 3.9.15

### Logic
1. Using `pandas` for data exploration
2. First, load data sources from `data` directory
3. I assume the data is comes from CDC and the schema is also CDC schema.
4. Once the data sources are loaded as dataframe, need to denormalize some `json structure` columns. Then sort in ascending to simulate the CDC data ingestion. All of the records are saved hitorically and have the active record/latest record for each unique record is identify by using `active_record` column.
5. Use `cards` dataframe as main data to be joining with `accounts` and `savings_accounts`. All of the records are saved hitorically and have the active record/latest record for each unique record is identify by using `active_record` column.
6. Joined table can be used to get the answers for points number 3 in the `README` instruction. 
    * How many transaction: 3
    * When each of them occurs:
    * how much the value of each transaction: 

### How to run locally
* install `requirements.txt` from root path (`../requirements.txt`)
* set `WORKDIR` environment variable, for example `/Users/<user>/Documents/dwh-coding-challenge`
* execute `python main.py`

### How to build docker
* go to root path, for example `/Users/<user>/Documents/dwh-coding-challenge`
* build image
    ```
    docker build -t dwh-coding-challenge:latest .
    ```
* run image
    ```
    docker run --rm -ti dwh-coding-challenge:latest python main.py
    ```