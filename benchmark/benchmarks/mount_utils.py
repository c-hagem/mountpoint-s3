import logging
from typing import Dict, Any

from omegaconf import DictConfig

from benchmarks.benchmark_config_parser import BenchmarkConfigParser
from benchmarks.mountpoint import mount_mp, cleanup_mp
from benchmarks.libfuse_stub import mount_stub, cleanup_stub

log = logging.getLogger(__name__)


def mount_filesystem(cfg: DictConfig, mount_dir: str) -> Dict[str, Any]:
    """
    Mount a filesystem based on the configuration mount_type.
    Supports both 'mountpoint' (S3) and 'stub' (libfuse stub) mounting.

    Args:
        cfg: Configuration object
        mount_dir: Directory to mount the filesystem at

    Returns:
        Dict containing mount metadata
    """
    config_parser = BenchmarkConfigParser(cfg)
    common_config = config_parser.get_common_config()
    mount_type = common_config['mount_type']

    log.info(f"Mounting filesystem with type: {mount_type}")

    if mount_type == 'mountpoint':
        log.info("Using Mountpoint S3 filesystem")
        mount_metadata = mount_mp(cfg, mount_dir)
        mount_metadata['mount_type'] = 'mountpoint'
        return mount_metadata

    elif mount_type == 'stub':
        log.info("Using libfuse stub filesystem")
        mount_metadata = mount_stub(cfg, mount_dir)
        mount_metadata['mount_type'] = 'stub'
        return mount_metadata

    else:
        raise ValueError(f"Unknown mount_type: {mount_type}. Supported types: 'mountpoint', 'stub'")


def cleanup_filesystem(mount_metadata: Dict[str, Any]) -> None:
    """
    Clean up the mounted filesystem based on its type.

    Args:
        mount_metadata: Dictionary containing mount information including mount_type
    """
    mount_type = mount_metadata.get('mount_type')
    mount_dir = mount_metadata.get('mount_dir')

    if not mount_type:
        log.warning("No mount_type found in metadata, attempting both cleanup methods")
        # Try both cleanup methods as fallback
        try:
            cleanup_mp(mount_dir)
        except Exception as e:
            log.debug(f"Mountpoint cleanup failed (expected if using stub): {e}")

        try:
            stub_pid = mount_metadata.get('target_pid')
            cleanup_stub(mount_dir, stub_pid)
        except Exception as e:
            log.debug(f"Stub cleanup failed (expected if using mountpoint): {e}")
        return

    log.info(f"Cleaning up {mount_type} filesystem at {mount_dir}")

    if mount_type == 'mountpoint':
        cleanup_mp(mount_dir)

    elif mount_type == 'stub':
        stub_pid = mount_metadata.get('target_pid')
        stub_process = mount_metadata.get('stub_process')

        # If we have the process object, terminate it directly
        if stub_process:
            try:
                stub_process.terminate()
                stub_process.wait(timeout=5)
                log.info(f"Terminated stub process {stub_process.pid}")
            except Exception as e:
                log.warning(f"Failed to terminate stub process cleanly: {e}")
                try:
                    stub_process.kill()
                    stub_process.wait(timeout=5)
                except Exception as kill_e:
                    log.error(f"Failed to kill stub process: {kill_e}")

        cleanup_stub(mount_dir, stub_pid)

    else:
        log.error(f"Unknown mount_type in cleanup: {mount_type}")


def get_mount_info(mount_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract relevant mount information for logging/reporting.

    Args:
        mount_metadata: Dictionary containing mount information

    Returns:
        Dict containing formatted mount information
    """
    mount_type = mount_metadata.get('mount_type', 'unknown')

    base_info = {
        'mount_type': mount_type,
        'mount_dir': mount_metadata.get('mount_dir'),
        'target_pid': mount_metadata.get('target_pid'),
    }

    if mount_type == 'mountpoint':
        base_info.update({
            'mp_version': mount_metadata.get('mp_version'),
            'mount_s3_command': mount_metadata.get('mount_s3_command'),
        })

    elif mount_type == 'stub':
        base_info.update({
            'num_files': mount_metadata.get('num_files'),
            'background_threads': mount_metadata.get('background_threads'),
            'read_size': mount_metadata.get('read_size'),
            'latency_enabled': mount_metadata.get('latency_enabled'),
            'stub_command': mount_metadata.get('stub_command'),
        })

    return base_info
