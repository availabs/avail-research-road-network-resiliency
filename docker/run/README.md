# Dockerized Environments

## Usage

Setup...

1. `./init`
2. `ln -s ../../config/dev.env .env`
3. `./up`
4. `./sync_rnr-analysis_venv`
5. `./down`

NOTE: You must run `./sync_rnr-analysis_venv` each time you install new dependencies.

After setup, just run `./up` to start the containers, and `./down` to shut them down.

## TODO

## [ ] Look into using Docker Compose Watch

The current setup should get us started,
however using Docker Compose Watch
may simplify deployment if can automate
rebuilding the Python virual environment
after `git pull`.

NOTE: This is not a priority because local dev does not currently use docker.

* https://docs.astral.sh/uv/guides/integration/docker/#developing-in-a-container
* https://github.com/astral-sh/uv-docker-example/blob/main/compose.yml
* https://docs.docker.com/compose/how-tos/file-watch/
* https://www.docker.com/blog/how-to-dockerize-your-python-applications
