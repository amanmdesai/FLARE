import logging
import shutil
import subprocess
from functools import lru_cache
from itertools import product
from pathlib import Path

import b2luigi as luigi
from b2luigi.core.utils import flatten_to_dict
from typing_extensions import Union

from flare.src.mc_production.generator_specific_methods import (
    K4RunMethods,
    MadgraphMethods,
    SeedMethods,
)
from flare.src.mc_production.mc_production_types import get_mc_production_types
from flare.src.utils.bracket_mappings import (
    BracketMappingCMDBuilderMixin,
    BracketMappings,
    check_if_path_matches_mapping,
    get_suffix_from_arg,
)
from flare.src.utils.env import get_dataprod_env_script, get_sourced_env
from flare.src.utils.tasks import OutputMixin, _linear_task_workflow_generator
from flare.src.utils.yaml import get_config

logger = logging.getLogger("luigi-interface")

SEEDS_MANIFEST = "seeds.txt"
PYTHIA8_MAX_SEED = 900_000_000


def get_seeds() -> list:
    """
    Returns the first `n_seeds` (all if 0) seeds of the `seeds_file` in the mc_production directory,
    or an empty list if no `seeds_file` is set. Seeds are whitespace separated, `#` starts a comment
    """
    config = luigi.get_setting("dataprod_config", default={})
    seeds_file = getattr(config, "seeds_file", "")
    if not seeds_file:
        return []

    path = luigi.get_setting("dataprod_dir") / seeds_file
    seeds = [
        int(seed)
        for line in path.read_text().splitlines()
        for seed in line.split("#", 1)[0].split()
    ]
    assert config.n_seeds <= len(
        seeds
    ), f"n_seeds = {config.n_seeds} but {path} only has {len(seeds)} seeds"
    seeds = seeds[: config.n_seeds or None]
    assert len(set(seeds)) == len(seeds), f"{path} contains duplicate seeds"
    assert all(
        0 < seed <= PYTHIA8_MAX_SEED for seed in seeds
    ), f"Seeds in {path} must be within [1, {PYTHIA8_MAX_SEED}] (Pythia8 Random:seed range)"
    return seeds


class MCProductionBaseTask(
    luigi.DispatchableTask,
    BracketMappingCMDBuilderMixin,
    MadgraphMethods,
    K4RunMethods,
    SeedMethods,
):
    """
    This base class is total generalised to be able to run on any N-stage MC production
    workflow.
    """

    prodtype = luigi.EnumParameter(enum=get_mc_production_types())
    datatype = luigi.Parameter()

    stage: str
    results_subdir: str

    @property
    def dataprod_env_script(self) -> Union[None, str]:
        return get_dataprod_env_script()

    @property
    def slurm_settings(self):
        settings = luigi.get_setting("slurm_settings", {})
        # if self.env_script:
        #     settings["export"] = "NONE"
        return settings

    @property
    def input_file_path(self):
        return Path(next(iter(self.get_all_input_file_names())))

    @property
    def _unparsed_output_file_name(self):
        """
        The raw output file name from the production_types.yaml. This is then checked inside the property
        output_file_name and transformed as required
        """
        return self.stage_dict["output_file"]

    @property
    def stage_dict(self):
        """
        The dictionary of information for this stage
        """
        return self.prodtype.value[self.stage]

    @property
    def prod_cmd(self):
        """
        The cmd for this stage of the MC production as defined inside
        the production_types.yaml
        """
        cmd = self.stage_dict["cmd"].format(*self.collect_cmd_inputs())
        seed = getattr(self, "seed", None)
        if seed is not None and self.stage_dict.get("seed_args"):
            cmd += " " + self.stage_dict["seed_args"].format(seed=seed)
        return cmd

    @property
    def tmp_output_parent_dir(self):
        """
        The tmp output dir where all outputs of this workflow will be kept
        """
        return Path(
            self.get_output_file_name(self.output_file_name)
        ).parent.with_suffix(".tmp")

    @property
    def b2luigi_parameter_output_file_name(self):
        params = [self.datatype, self.card_name, self.edm4hep_name]
        filtered_params = [p for p in params if p != "default"]
        return "_".join(filtered_params)

    @property
    def output_file_name(self):
        """
        The output file may be dependent on a datatype, card_name or edm4hep_name parameters so must determine if the output
        file name needs to be parsed and transformed or if we can return the unparsed output file name
        """
        mapping = BracketMappings.determine_bracket_mapping(
            self._unparsed_output_file_name
        )
        if mapping == BracketMappings.datatype_parameter:
            suffix = get_suffix_from_arg(self._unparsed_output_file_name)
            return f"{self.datatype}{suffix}"
        elif mapping == BracketMappings.b2luigi_detemined_parameter:
            suffix = get_suffix_from_arg(self._unparsed_output_file_name)  # eg .root
            prefix = self.b2luigi_parameter_output_file_name
            return f"{prefix}{suffix}"
        else:
            return self._unparsed_output_file_name

    def copy_input_file_to_output_dir(self, path, return_destination=False):
        """
        This function serves to copy a file from analysis/mc_production/ to
        the tmp output dir for historical book keeping
        """
        source = Path(path)
        self.tmp_output_parent_dir.mkdir(parents=True, exist_ok=True)
        destination = self.tmp_output_parent_dir / source.name
        shutil.copy(source, destination)
        if return_destination:
            return destination

    def get_file_paths(self):
        return luigi.get_setting("dataprod_dir").glob("*")

    @property
    def unparsed_args(self):
        return self.stage_dict["args"]

    def bm_output(self) -> Path:
        # Create the path to the tmp dir
        return self.tmp_output_parent_dir / self.output_file_name

    def bm_input(self) -> Path:

        return list(self.get_all_input_file_names())[0]

    def bm_datatype_parameter(self, arg) -> Path:
        arg = arg.replace(BracketMappings.datatype_parameter, self.datatype)
        return self.seed_input_file(
            self._find_file_path_given_arg_and_bracketmapping(
                arg=arg, bracket_mapping=BracketMappings.datatype_parameter
            )
        )

    def bm_free_name(self, arg):
        return self.seed_input_file(
            self._find_file_path_given_arg_and_bracketmapping(
                arg=arg, bracket_mapping=BracketMappings.free_name
            )
        )

    def bm_datatype_parameter_stem(self, arg) -> str:
        return self.datatype

    def bm_free_name_use_copied_output(self, arg) -> Path:
        path = self._find_file_path_given_arg_and_bracketmapping(
            arg=arg, bracket_mapping=BracketMappings.free_name_use_copied_output
        )
        return self.copy_input_file_to_output_dir(path, return_destination=True)

    def _find_file_path_given_arg_and_bracketmapping(
        self, arg: str, bracket_mapping: BracketMappings
    ) -> Path:
        file_paths = [f for f in self.get_file_paths()]
        # Find the associated file using the check_if_path_matches_mapping function
        file_path = [
            str(f)
            for f in file_paths
            if check_if_path_matches_mapping(arg, f, bracket_mapping)
        ]
        length = len(file_path)
        if length == 0:
            raise IndexError(
                f"There is no file associated with {arg} inside {str(luigi.get_setting('dataprod_dir'))}."
                " The framework will exit, ensure this file is present and try again."
            )
        elif length == 1:
            # We copy this file to the tmp output dir so we have a history of what input files were used
            path = file_path[0]
            self.copy_input_file_to_output_dir(path)
            return path
        else:
            # More than one, we assume we are looping over a parameter of this class
            if "card" in arg:
                path = [p for p in file_path if self.card_name in Path(p).stem][0]
            elif "edm4hep" in arg:
                path = [p for p in file_path if self.edm4hep_name in Path(p).stem][0]
            elif self.datatype in arg:
                path = [p for p in file_path if self.datatype == Path(p).stem][0]
            else:
                raise FileNotFoundError(
                    f"The file associated with {arg} is unknown to flare. The found paths are {file_path}."
                    f" This may occur if there are multiple files being picked up by flare for {arg}"
                )

            self.copy_input_file_to_output_dir(path)
            return path

    def process(self):
        """
        This process function essentially acts as the `run` function for a dispatchable task.

        Here the function collects the appropriate cmd to be submitted using the information provided from
        production_types.yaml. Then when ran on the batch system of your choosing, the subprocess.check_call
        will run the cmd and make sure it completes. Once this is done, the output path is moved from the
        tmp folder to the correct folder at which point b2luigi flags the job as done
        """

        logger.info(f"Command to be ran \n\n {self.prod_cmd} \n\n")
        # Run any required pre_run methods for this specific stage for this specific prodtype
        self.pre_run()
        # Run the cmd in the tmp directory
        subprocess.check_call(
            self.prod_cmd,
            cwd=self.tmp_output_parent_dir,
            shell=True,
            env=get_sourced_env(self.dataprod_env_script),
        )
        # Run any required on_completion methods for this specific stage for this specific prodtype
        self.on_completion()

        # Get final output dir
        target = self.tmp_output_parent_dir.with_suffix("")

        logger.info(f"Moving {self.tmp_output_parent_dir} -> {target}")

        # Move the contents of the tmp dir to the output dir. Not we cannot just move the
        # directory as b2luigi's batch submitter saves the executable_wrapper.sh to the output dir
        shutil.copytree(self.tmp_output_parent_dir, target, dirs_exist_ok=True)
        # Delete output dir
        shutil.rmtree(self.tmp_output_parent_dir)

    def on_completion(self):
        """
        This function is intended to run the required functions to be ran after the main cmd
        for this stage detailed in production_types.yaml
        """
        try:
            func_names = self.stage_dict["on_completion"]
        except KeyError:
            return

        for func_name in func_names:
            if hasattr(self, func_name):
                func = getattr(self, func_name)
                func()

    def pre_run(self):
        """
        This function is intended to run the required functions to be ran prior to the main cmd
        for this stage detailed in production_types.yaml
        """
        try:
            func_names = self.stage_dict["pre_run"]
        except KeyError:
            return

        for func_name in func_names:
            if hasattr(self, func_name):
                func = getattr(self, func_name)
                func()

    def output(self):
        yield self.add_to_output(self.output_file_name)


class MCProductionWrapper(OutputMixin, luigi.DispatchableTask):
    """
    This task simply compiles the outputs from the required tasks into a single folder.

    This is necessary because the FCC analysis tools works by passing the folder and looking for the expected
    input parameters to the workflow.
    """

    prodtype = luigi.Parameter()

    @property
    def slurm_settings(self):
        settings = luigi.get_setting("slurm_settings", {})
        env_script = luigi.get_setting("dataprod_config").global_env_script_path
        if env_script:
            settings["export"] = "ALL"
        return settings

    @property
    def results_subdir(self):
        return luigi.get_setting("results_subdir")

    @property
    def input_paths(self):
        return list(self.get_all_input_file_names())

    @property
    def inject_stage1_dependency_task(self) -> None:
        return None

    @property
    def seeded_links(self):
        """
        (source, link name) for each seed's output. Links are grouped into one directory per process,
        which FCCAnalyses reads as a single sample
        """
        for task in self.requires():
            name = Path(task.output_file_name)
            source = task.get_output_file_name(task.output_file_name)
            yield source, f"{name.stem}/{name.stem}_seed{task.seed}{name.suffix}"

    def link_seeded_outputs(self):
        # Symlink rather than copy, as copying every seed's output would double the disk usage
        for source, name in self.seeded_links:
            target = Path(self.get_output_file_name(name))
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.is_symlink():
                target.unlink()
            logger.info(f"Linking {source} -> {target}")
            target.symlink_to(source)

        events = luigi.get_setting("dataprod_config").events_per_seed
        Path(self.get_output_file_name(SEEDS_MANIFEST)).write_text(
            f"# events_per_seed: {events or 'as set in card'}\n"
            + "\n".join(str(seed) for seed in get_seeds())
            + "\n"
        )

    def process(self):
        if get_seeds():
            self.link_seeded_outputs()
            return
        # Copy the file and its metadata (hence copy2) to the output directory
        for input_file in self.input_paths:
            source = Path(input_file)
            target = self.get_output_file_name(source.name)
            logger.info(f"Copying {source} -> {target}")
            shutil.copy2(source, target)

    def output(self):
        if get_seeds():
            # The manifest must be the first output: the stage1 analysis takes the parent of its first input as inputDir
            yield self.add_to_output(SEEDS_MANIFEST)
            for _, name in self.seeded_links:
                yield self.add_to_output(name)
            return

        for input_file in self.input_paths:
            path = Path(input_file)
            yield self.add_to_output(path.name)

    def requires(self):
        dataprod_config = luigi.get_setting("dataprod_config")
        seeds = get_seeds() or [None]
        # If the prodtype is default i.e wasn't defined globally
        # we must call the default_prodtype requires function
        if self.prodtype == "default":
            datatypes_dict = flatten_to_dict(dataprod_config.datatype)
            datatypes = list(datatypes_dict.keys())

            for datatype, card, edm4hep, seed in product(
                datatypes,
                dataprod_config.card,
                dataprod_config.edm4hep,
                seeds,
            ):
                prodtype = datatypes_dict[datatype]["prodtype"]

                yield get_last_stage_task(
                    inject_stage1_dependency=self.inject_stage1_dependency_task,
                    prodtype=prodtype,
                )(
                    prodtype=get_mc_production_types()[prodtype],
                    datatype=datatype,
                    card_name=card,
                    edm4hep_name=edm4hep,
                    **({} if seed is None else {"seed": seed}),
                )

        else:
            for datatype, card, edm4hep, seed in product(
                dataprod_config.datatype,
                dataprod_config.card,
                dataprod_config.edm4hep,
                seeds,
            ):
                yield get_last_stage_task(
                    inject_stage1_dependency=self.inject_stage1_dependency_task
                )(
                    prodtype=get_mc_production_types()[self.prodtype],
                    datatype=datatype,
                    card_name=card,
                    edm4hep_name=edm4hep,
                    **({} if seed is None else {"seed": seed}),
                )


def _get_mc_prod_stages(prodtype=None) -> dict:
    """
    Returns
    --------
    `prod_dict` : dict[str, dict]
        Returned dictionary is that specific to the production type as per src/mc_production/production_types.yaml'
    """
    requested_prodtype = (
        prodtype or luigi.get_setting("dataprod_config").global_prodtype
    )
    try:
        return get_config("production_types.yaml", "flare/src/mc_production")[
            requested_prodtype
        ]
    except KeyError:
        raise KeyError(requested_prodtype)


@lru_cache(typed=True)
def get_mc_prod_stages_dict(inject_stage1_dependency=None, prodtype=None) -> dict:
    """
    Get the ordered dictionary of MCProduction tasks.

    Parameters
    -----------
    `inject_stage1_dependency`: luigi.Task, default=None
        If the stage1 task of the MC Production needs to require another task, massing a Task to this parameter will set that dependency

    Returns
    --------
    `tasks` : dict[Any, luigi.Task]
        The returned ordered dictionary has keys as per the `stages` list passed to _class_generator_closure_function.
        The values are the associated luigi Tasks that are created as child classes of `MCProductionBaseTask`

    Note
    -----
    For the MC Production of FLARE, the following configuration is passed to `_class_generator_closure_function`:

    ```
    _linear_task_workflow_generator(
        stages=_get_mc_prod_stages(),
        class_name="MCProduction",
        base_class=MCProductionBaseTask,
        inject_stage1_dependency=inject_stage1_dependency
    )
    ```
    """
    stages = list(_get_mc_prod_stages(prodtype=prodtype))
    class_name = "MCProduction"
    class_name += prodtype.capitalize() if prodtype else ""
    # Every stage needs the seed parameter, as clone() only passes on parameters the upstream task declares
    class_attrs = {
        stage: ({"seed": luigi.IntParameter()} if get_seeds() else {})
        for stage in stages
    }
    class_attrs[stages[-1]].update(
        {
            "card_name": luigi.Parameter(default="default"),
            "edm4hep_name": luigi.Parameter(default="default"),
        }
    )
    return _linear_task_workflow_generator(
        stages=stages,
        class_name=class_name,
        base_class=MCProductionBaseTask,
        class_attrs=class_attrs,
        inject_stage1_dependency=inject_stage1_dependency,
    )


def get_last_stage_task(inject_stage1_dependency=None, prodtype=None):
    """
    Returns the last luigi Task inside `get_mc_prod_stages_dict`
    """
    return next(
        reversed(
            get_mc_prod_stages_dict(
                inject_stage1_dependency=inject_stage1_dependency, prodtype=prodtype
            ).values()
        )
    )
