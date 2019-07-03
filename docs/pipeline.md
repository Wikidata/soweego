# Run the pipeline
It's useful when you need to run all the _soweego_ steps. It does not require you to have the project in your machine: it is an already built docker image.
Editing the credentials is all you need to do to chose the database.

This environment is a docker container that executes `python -m soweego pipeline` command. Obviously, it can be run with custom options.
Note: the code is put into the image during the build process.

### What do you need to run it?
* [Docker](https://www.docker.com/get-started)
* Database credentials file, like `${PROJECT_ROOT}/soweego/importer/resources/db_credentials.json`.

### How do you run it?
1. In your terminal, move to the project root;
1. Launch `./scripts/docker/launch_pipeline.sh`;
1. The script will ask you some parameters.
1. The pipeline is now running!

#### launch_pipeline.sh options
| **Option** | **Expected Value** | **Default Value** | **Description** |
|---|---|---|---|
| -s | directory path | `/tmp/soweego_shared` |Tells docker which folder in your machine will be shared with _soweego_ container. |
| target (argument, not an option) | one of the available targets name (atm. musicbrainz, discogs. imdb) | None | Tells _soweego_ which database will go through the whole process.|
| --validator / --no-validator | No value needed. The option you choose among the two is the value itself. | --no-validator | Tells _soweego_ to run or not the validator step.|
| --importer / --no-importer | No value needed. The option you choose among the two is the value itself. | --importer | Tells _soweego_ to run or not the importer step.|
| --linker / --no-linker | No value needed. The option you choose among the two is the value itself. | --linker | Tells _soweego_ to run or not the linker step.|
| --upload / --no-upload | No value needed. The option you choose among the two is the value itself. | --upload | Tells _soweego_ to upload or not the computed links among wikidata and the target to wikidata.|

