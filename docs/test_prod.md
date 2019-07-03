# Set up test and production environments

## Get started
1. [Install Docker](https://www.docker.com/get-started);
2. In your command line launch the following commands:
```bash
git clone https://github.com/Wikidata/soweego.git
cd soweego
```

## Test environment
It's useful when you are developing or testing some features.
In this environment, you don't need to be afraid of breaking stuff.

It provides you with a MariaDB instance and a BASH shell ready to run _soweego_ CLI commands.
You are free to change _soweego_ code while the shell is running, the code is synced.

### What do you need to run it?
* [Docker](https://www.docker.com/get-started)
* [Docker Compose](https://docs.docker.com/compose/install/)

### How do you run it?
1. In your terminal, move to the project root;
1. Launch `./scripts/docker/launch_test.sh`;
1. You are now in a Docker container BASH shell with a fully working soweego instance;
1. Run `cd soweego`;
1. You are set. To check if it's working, try `python -m soweego`.

#### launch_test.sh options
| **Option** | **Expected Value** | **Default Value** | **Description** |
|---|---|---|---|
| -s | directory path | `/tmp/soweego_shared` |Tells docker which folder in your machine will be shared with soweego container. |


### How do you connect with the local database instance?
The test environment comes with a running [MariaDB](https://mariadb.com/) instance.
To query it from your terminal:
1. `docker exec -it soweego_db_1 /bin/bash`;
2. `mysql -uroot -hlocalhost -pdba soweego`.

## Production environment
It's useful when you need to run _soweego_ against the Wikimedia database. It is also helpful to run the system against a custom database.
Editing the credentials is all you need to do to chose the database.
Note: you need access to the Wikimedia infrastructure to run _soweego_ on it.

This environment provides you with a BASH shell ready to run _soweego_ CLI commands.
You are free to change _soweego_ code while the shell is running, the code is synced.

### What do you need to run it?
* [Docker](https://www.docker.com/get-started)
* Database credentials file, like `${PROJECT_ROOT}/soweego/importer/resources/db_credentials.json`.

### How do you run it?
1. In your terminal, move to the project root;
1. Launch `./scripts/docker/launch_prod.sh`;
1. You are now in a Docker container BASH shell with a fully working _soweego_ instance;
1. Run `cd soweego`;
1. You are set. To check if it's working, try `python -m soweego`.

#### launch_prod.sh options
| **Option** | **Expected Value** | **Default Value** | **Description** |
|---|---|---|---|
| -s | directory path | `/tmp/soweego_shared` |Tells docker which folder in your machine will be shared with _soweego_ container. |
| -c | file path | `${PROJECT_ROOT}/soweego/importer/resources/db_credentials.json` |Sets which file in your machine _soweego_ will read for database credentials.|

