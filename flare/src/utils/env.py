import os
import subprocess

import b2luigi as luigi


def get_dataprod_env_script():
    config = luigi.get_setting("dataprod_config", default={})
    env_script = getattr(config, "global_env_script_path", "")
    return luigi.get_setting("dataprod_dir") / env_script if env_script else None


def get_sourced_env(setup_script, extra_args="", preserve=None):
    """Source `setup_script` in a clean `env -i` bash shell and return the resulting environment."""
    if setup_script is None:
        return None

    preserve = preserve or {}
    base_preserved = {
        "HOME": os.environ.get("HOME", ""),
        "USER": os.environ.get("USER", ""),
        "TERM": os.environ.get("TERM", "dumb"),
    }
    base_preserved.update(preserve)

    preserved_exports = " ".join(f"{k}={v!r}" for k, v in base_preserved.items())

    command = (
        f"env -i {preserved_exports} "
        f"bash -c 'source {setup_script} {extra_args} && env -0'"
    )

    output = subprocess.check_output(
        ["bash", "-c", command],
        env={},  # belt-and-suspenders: don't inherit here either
        universal_newlines=False,
    )

    env = {}
    for line in output.split(b"\0"):
        if not line:
            continue
        key, _, value = line.partition(b"=")
        env[key.decode()] = value.decode()
    return env
