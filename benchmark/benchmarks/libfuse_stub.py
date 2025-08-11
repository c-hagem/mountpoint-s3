
import logging
import os
import subprocess
import signal
from typing import Dict, Any

from omegaconf import DictConfig

from benchmarks.benchmark_config_parser import BenchmarkConfigParser

logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO').upper())
log = logging.getLogger(__name__)


def cleanup_stub(mount_dir, stub_pid=None):
    """Clean up the libfuse stub filesystem"""
    if mount_dir is not None:
        log.info(f"Cleaning up stub filesystem at {mount_dir}")
        try:
            subprocess.check_output(["fusermount", "-u", mount_dir], stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            # Try umount as fallback
            try:
                subprocess.check_output(["umount", mount_dir], stderr=subprocess.DEVNULL)
            except subprocess.CalledProcessError as e:
                log.warning(f"Failed to unmount {mount_dir}: {e}")

        try:
            os.rmdir(mount_dir)
        except OSError as e:
            log.warning(f"Failed to remove directory {mount_dir}: {e}")

    if stub_pid is not None:
        try:
            os.kill(stub_pid, signal.SIGTERM)
            log.info(f"Terminated stub process {stub_pid}")
        except ProcessLookupError:
            log.debug(f"Stub process {stub_pid} already terminated")


def mount_stub(cfg: DictConfig, mount_dir: str) -> Dict[str, Any]:
    """
    Mounts a stubbed filesystem using libfuse stub.
    """
    config_parser = BenchmarkConfigParser(cfg)
    common_config = config_parser.get_common_config()

    # Get stub-specific configuration
    stub_config = config_parser.get_stub_config()
    stub_binary = stub_config['stub_binary']
    num_files = stub_config['num_files']
    background_threads = stub_config['background_threads']
    read_size = stub_config['read_size']

    # Latency configuration
    latency_config = stub_config['latency']
    use_latency = latency_config['enabled']
    latency_mean = latency_config['mean']  # microseconds
    latency_stddev = latency_config['stddev']  # microseconds

    log.info(f"Mounting libfuse stub filesystem at {mount_dir}")
    log.info(f"Stub config - files: {num_files}, threads: {background_threads}, read_size: {read_size}")

    if use_latency:
        log.info(f"Latency simulation enabled - mean: {latency_mean}µs, stddev: {latency_stddev}µs")

    # Create mount directory
    os.makedirs(mount_dir, exist_ok=True)

    # Set up environment variables for the stub
    stub_env = os.environ.copy()
    stub_env['C_STUB_NUMFILES'] = str(num_files)
    stub_env['C_STUB_BACKGROUND_THREADS'] = str(background_threads)
    stub_env['C_STUB_READSIZE'] = str(read_size)

    if use_latency:
        stub_env['STUB_DISTR'] = 'normal'
        stub_env['STUB_DISTR_MEAN'] = str(latency_mean)
        stub_env['STUB_DISTR_STDDEV'] = str(latency_stddev)

    # Build subprocess arguments
    subprocess_args = [
        stub_binary,
        mount_dir,
        '-f',  # Run in foreground to capture output
    ]

    log.info(f"Starting stub with args: {subprocess_args}")
    log.info(f"Stub environment: {dict((k, v) for k, v in stub_env.items() if k.startswith(('C_STUB_', 'STUB_')))}")

    # Start the stub filesystem in the background
    try:
        stub_process = subprocess.Popen(
            subprocess_args,
            env=stub_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Give it a moment to start up
        import time
        time.sleep(0.5)

        # Check if the process is still running
        if stub_process.poll() is not None:
            stdout, stderr = stub_process.communicate()
            raise RuntimeError(f"Stub process failed to start. stdout: {stdout.decode()}, stderr: {stderr.decode()}")

        log.info(f"Stub filesystem mounted successfully with PID {stub_process.pid}")

        return {
            "mount_dir": mount_dir,
            "stub_command": " ".join(subprocess_args),
            "stub_env": stub_env,
            "target_pid": stub_process.pid,
            "stub_process": stub_process,
            "num_files": num_files,
            "background_threads": background_threads,
            "read_size": read_size,
            "latency_enabled": use_latency,
        }

    except Exception as e:
        # Clean up on failure
        try:
            os.rmdir(mount_dir)
        except OSError:
            pass
        raise RuntimeError(f"Failed to mount stub filesystem: {e}")
