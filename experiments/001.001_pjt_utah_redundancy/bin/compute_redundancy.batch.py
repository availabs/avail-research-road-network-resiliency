import os
import re
import asyncio

DEFAULT_CONCURRENCY = 4

osm_file_prefix = 'vehicle-nonservice-roadways_county-subdivision-'

this_dir = os.path.dirname(
    os.path.abspath(__file__)
)

worker_path = os.path.join(this_dir, 'replicating-Utah-redundancy.py')

async def worker(name, queue):
    while True:
        # Get a "work item" out of the queue.
        file = await queue.get()

        print('worker:', name, '; file:', file)

        cmd = ['python', worker_path, '--filename', file]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            shell=False
        )

        stdout, stderr = await proc.communicate()

        user_warning_regex = r'(UserWarning: The inferred zoom level of)|(warnings.warn)'

        filtered_stderr = '\n'.join(
            [line for line in stderr.decode().split('\n') if not re.search(user_warning_regex, line)]
        )

        print(f'[{cmd!r} exited with {proc.returncode}]')
        if stdout:
            print(f'[stdout]\n{stdout.decode()}')
        if stderr:
            print(f'[stderr]\n{filtered_stderr}')

        # Notify the queue that the "work item" has been processed.
        queue.task_done()

async def manager(filenames, concurrency=DEFAULT_CONCURRENCY):
    # Create a queue that we will use to store our "workload".
    queue = asyncio.Queue()

    osm_pbf_re = re.compile(r'\.osm\.pbf$')

    osm_pbf_files = sorted([
        f for f in filenames
        if re.search(osm_pbf_re, f)
    ])

    for file in osm_pbf_files:
        queue.put_nowait(file)

    # Create three worker tasks to process the queue concurrently.
    tasks = []
    for i in range(concurrency):
        task = asyncio.create_task(worker(f'worker-{i}', queue))
        tasks.append(task)

    # Wait until the queue is fully processed.
    await queue.join()

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()

    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)

def main(filenames, concurrency=DEFAULT_CONCURRENCY):
    asyncio.run(
        manager(filenames=filenames, concurrency=concurrency)
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Compute road redundancy for the provided OSM PBF files.'
    )

    parser.add_argument(
        '-f',
        '--filenames',
        nargs='+',
        help='Path to OSM PBF files to process.',
        required=True
    )

    parser.add_argument(
        '-c',
        '--concurrency',
        type=int,
        default=DEFAULT_CONCURRENCY,
        help='Concurrency level.',
    )

    args = parser.parse_args()

    main(
        filenames=args.filenames,
        concurrency=args.concurrency
    )