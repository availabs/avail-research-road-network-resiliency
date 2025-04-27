import os
import time
from typing import Callable, Tuple

from prefect import get_run_logger, task

import docker
import docker.errors
from docker.models.containers import Container

# FIXME: Hard-coding these for now. Need to implement a centralized store for canonical versions.
DEFAULT_OSM_VERSION = "nonservice-roadways-buffer-50mi-state-36_us-250101"

OSRM_DATA_DIR = os.path.abspath("../../data/processed/osrm")


@task(name="Create OSRM Container")
def start_osrm_container_task(
    osm_version: str = DEFAULT_OSM_VERSION,
) -> Tuple[str, Callable[[], None]]:
    """
    Starts an OSRM Docker container with dynamic port allocation and returns
    the container URL and a cleanup function.

    Includes logging and basic error/edge case handling.
    """
    logger = get_run_logger()
    client = None
    container: Container = (
        None  # Initialize container to None to handle errors before creation
    )

    osrm_mount_dir = os.path.join(OSRM_DATA_DIR, osm_version)

    logger.info("Attempting to connect to Docker daemon.")
    try:
        # Connect to the Docker daemon (assumes socket is mounted or container is privileged)
        client = docker.from_env()
        client.ping()  # Verify connection works
        logger.info("Successfully connected to Docker daemon.")
    except Exception as e:
        logger.error(
            f"Failed to connect to Docker daemon. Ensure Docker is running and accessible: {e}"
        )
        raise  # Re-raise the exception as connection is essential for the task

    try:
        logger.info(
            f"Starting OSRM container using image ghcr.io/project-osrm/osrm-backend with data version: {osm_version}"
        )
        # Start a container with dynamic port allocation
        container = client.containers.run(
            "ghcr.io/project-osrm/osrm-backend",
            ports={"5000/tcp": None},  # Dynamically assign a host port
            entrypoint=[
                "osrm-routed",
                "--max-matching-size",
                "100000",
                f"/data/{osm_version}.osrm",
            ],
            volumes=[f"{osrm_mount_dir}:/data/"],
            detach=True,  # Run in detached mode
            # Consider adding restart_policy if needed, e.g., on-failure
            # restart_policy={"Name": "on-failure", "MaximumRetryCount": 2},
            # Add auto_remove=True if you want Docker to clean up on container exit (even on failure)
            # auto_remove=True,
            # Add a name to the container for easier identification
            # name=f"rnr-osrm-container-{OSRM_OSM_VERSION}",  # Simple naming, consider more unique names for concurrent runs
        )

        # for log in container.logs(stream=True, follow=True):
        #     print(log.decode("utf-8").strip())

        logger.info(f"OSRM container started successfully with ID: {container.id}")

        # Get the assigned host port
        # It might take a moment for the port to be assigned and visible
        max_attempts = 10
        attempt = 0
        host_port = None
        while attempt < max_attempts:
            try:
                container.reload()  # Refresh container information
                # Accessing the port info requires the container to be in a state where ports are mapped
                host_port_mapping = (
                    container.attrs.get("NetworkSettings", {})
                    .get("Ports", {})
                    .get("5000/tcp")
                )

                if host_port_mapping and host_port_mapping[0].get("HostPort"):
                    host_port = host_port_mapping[0]["HostPort"]
                    logger.info(
                        f"OSRM container {container.id} mapped to host port: {host_port}"
                    )
                    break  # Exit loop once port is found
            except Exception as port_e:
                logger.warning(
                    f"Attempt {attempt + 1}/{max_attempts} to get host port failed: {port_e}"
                )

            attempt += 1
            if attempt < max_attempts:
                time.sleep(2)  # Wait before retrying

        if not host_port:
            logger.error(
                f"Failed to get host port mapping for container {container.id} after {max_attempts} attempts."
            )
            # Attempt cleanup before raising error
            if container:
                try:
                    logger.info(
                        f"Attempting cleanup of failed container {container.id}..."
                    )
                    container.stop()
                    container.remove()
                    logger.info(f"Cleaned up container {container.id}.")
                except docker.errors.NotFound:
                    logger.warning(
                        f"Container {container.id} not found during cleanup, may have already been removed."
                    )
                except Exception as cleanup_e:
                    logger.error(
                        f"Failed to cleanup container {container.id} after port mapping error: {cleanup_e}"
                    )
            raise ConnectionError("Failed to get host port mapping for OSRM container.")

        osrm_url = f"http://127.0.0.1:{host_port}"
        logger.info(f"OSRM service URL: {osrm_url}")

        def cleanup():
            """Stops and removes the OSRM container."""
            logger.info(
                f"Attempting to clean up OSRM container with ID: {container.id}"
            )
            try:
                if (
                    container
                ):  # Ensure container object exists before trying to stop/remove
                    container.stop()
                    logger.info(f"OSRM Container {container.id} stopped.")
                    container.remove()
                    logger.info(f"OSRM Container {container.id} removed.")
                else:
                    logger.warning("No OSRM container object found for cleanup.")
            except docker.errors.NotFound:
                logger.warning(
                    f"Container {container.id} not found during cleanup, may have already been removed."
                )
            except Exception as cleanup_e:
                logger.error(f"Error during OSRM container cleanup: {cleanup_e}")
                # Consider raising the exception if cleanup failure is critical

        # Return the URL and the cleanup function
        return osrm_url, cleanup

    except docker.errors.ImageNotFound as e:
        logger.error(
            f"Docker image not found: ghcr.io/project-osrm/osrm-backend. Please ensure the image exists locally or is accessible."
        )
        raise  # Re-raise the specific error
    except docker.errors.APIError as e:
        logger.error(f"Docker API error occurred: {e}")
        # Attempt cleanup if container object was created before the error
        if container:
            try:
                logger.info(
                    f"Attempting to stop and remove container {container.id} due to API error."
                )
                container.stop()
                container.remove()
                logger.info(f"Container {container.id} cleaned up after API error.")
            except docker.errors.NotFound:
                logger.warning(
                    f"Container {container.id} not found during cleanup after API error."
                )
            except Exception as cleanup_e:
                logger.error(
                    f"Failed to cleanup container {container.id} after API error: {cleanup_e}"
                )
        raise  # Re-raise the specific error
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during OSRM container setup or execution: {e}"
        )
        # Attempt cleanup if container object was created before the error
        if container:
            try:
                logger.info(
                    f"Attempting to stop and remove container {container.id} due to unexpected error."
                )
                container.stop()
                container.remove()
                logger.info(
                    f"Container {container.id} cleaned up after unexpected error."
                )
            except docker.errors.NotFound:
                logger.warning(
                    f"Container {container.id} not found during cleanup after unexpected error."
                )
            except Exception as cleanup_e:
                logger.error(
                    f"Failed to cleanup container {container.id} after unexpected error: {cleanup_e}"
                )
        raise  # Re-raise the original exception
