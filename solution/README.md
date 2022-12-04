## DWH Challenge

### Prerequisites
* python 3.9.15

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