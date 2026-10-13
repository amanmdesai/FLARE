import re
import shutil
import subprocess
from pathlib import Path

from b2luigi import get_setting


class MadgraphMethods:
    """
    This class contains the methods required to run the madgraph workflow for MC generation.

    These methods match those inside production_types.yaml.

    This class is intended to be inherited by the MCProductionBaseTask. This is to avoid polluting
    the base task with too many methods.

    """

    @property
    def tmp_output_parent_dir(self):
        raise NotImplementedError

    @property
    def input_file_path(self):
        raise NotImplementedError

    def madgraph_move_contents_to_tmp_output(self):
        """
        This method is ran after the mg5_aMC command to allow for the additional step of unzipping the unweighted_events.lhe.gz
        file and output it.

        Note we are specifying absolute paths as to not make a mistake and misplace the output file

        """
        cwd_dirs = self.tmp_output_parent_dir.glob("*")

        not_output_dirs = [f for f in cwd_dirs if f.is_dir()]

        assert (
            len(not_output_dirs) == 1
        ), "More than one output directory was made in madgraph running"

        madgraph_dir = not_output_dirs[0]

        zip_file = Path(madgraph_dir, "Events", "run_01", "unweighted_events.lhe.gz")

        cmd = " ".join(
            [
                "gunzip -c",
                str(zip_file),
                ">",
                f"{str(self.tmp_output_parent_dir)}/{self.output_file_name}",
            ]
        )

        subprocess.check_call(cmd, cwd=self.tmp_output_parent_dir, shell=True)

    def madgraph_copy_lhe_file_to_cwd(self):
        """
        When running DelphesPythia8_EMD4HEP, the output .lhe file from stage 1 must be in the cwd for the process to work.

        This method will copy the file as to keep everything in a single place. This is in opposition to symlinking

        """
        input_file_name = Path(self.input_file_path).name

        shutil.copyfile(
            self.input_file_path, dst=f"{self.tmp_output_parent_dir}/{input_file_name}"
        )


class K4RunMethods:
    """Methods required to run k4run-based stages (e.g. CLD reconstruction) for MC generation."""

    @property
    def tmp_output_parent_dir(self):
        raise NotImplementedError

    @property
    def output_file_name(self):
        raise NotImplementedError

    def cp_sandbox_files(self):
        """Symlinks the `k4run_sandbox` directory's contents into the stage's working directory."""
        sandbox_path = get_setting("dataprod_config").k4run_sandbox
        if not sandbox_path:
            return

        for path in Path(sandbox_path).glob("*"):
            link = self.tmp_output_parent_dir / path.relative_to(sandbox_path)
            if not link.exists():
                link.symlink_to(path)

    def cld_reco_rename_output(self):
        """Renames CLDReconstruction.py's fixed "<outputBasename>_REC.edm4hep.root" to FLARE's expected output_file_name."""
        produced = self.tmp_output_parent_dir / f"{self.datatype}_REC.edm4hep.root"
        target = self.tmp_output_parent_dir / self.output_file_name
        if produced != target:
            produced.rename(target)


class SeedMethods:
    """Methods required to run one MC production job per seed listed in the `seeds_file`."""

    @property
    def tmp_output_parent_dir(self):
        raise NotImplementedError

    def seed_input_file(self, path):
        """Writes a copy of a Pythia8 (.cmd) or Delphes (.tcl) card carrying this task's seed to the working directory."""
        seed = getattr(self, "seed", None)
        if seed is None:
            return path

        source = Path(path)
        text = source.read_text()
        if source.suffix == ".cmd":
            seeded = reseed_pythia8_card(
                text, seed, get_setting("dataprod_config").events_per_seed
            )
        elif source.suffix == ".tcl":
            seeded = re.sub(
                r"^(\s*set\s+RandomSeed\s+)\S+", rf"\g<1>{seed}", text, flags=re.M
            )
        else:
            return path

        if seeded == text:
            return path

        destination = self.tmp_output_parent_dir / source.name
        destination.write_text(seeded)
        return destination


def reseed_pythia8_card(text, seed, n_events=0):
    """Sets `Random:seed` (and `Main:numberOfEvents` if `n_events`) in a Pythia8 card, appending any that are missing."""
    settings = {
        "random:setseed": "Random:setSeed = on",
        "random:seed": f"Random:seed = {seed}",
    }
    if n_events:
        settings["main:numberofevents"] = f"Main:numberOfEvents = {n_events}"

    lines, seen = [], set()
    for line in text.splitlines():
        key = line.split("!", 1)[0].split("=", 1)[0].strip().lower()
        if key in settings:
            seen.add(key)
            line = settings[key]
        lines.append(line)
    lines += [value for key, value in settings.items() if key not in seen]
    return "\n".join(lines) + "\n"
