#!/usr/bin/env python
"""
Datasets from the StopsCompressed analysis.

The datasets are defined by a Google Spreadsheet:

Example:
    ./datasets.py list 
    ./datasets.py stage MET_Run2016B_ver2_HIPM_UL --type=MiniAOD --max-files=1
"""
import asyncio
import csv
import io
import json
import logging
import os
import pathlib
import shutil
import subprocess
import urllib.request

import click

logging.basicConfig(
    format="%(asctime)s - %(levelname)8s - %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

log = logging.getLogger(__name__)

# CMS datataking periods
PERIODS = [
    "Run2016preVFP",
    "Run2016postVFP",
    "Run2017",
    "Run2018",
]

# data tiers
TYPES = [
    "MiniAOD",
    "NanoAOD",
    "ReNanoAOD",
    "NTuple",
]

# the dataset spreadsheet
GOOGLE_DOCID = "1ddNADBoH1f-bL9faXes15c4hPGvB9ugLOv_7VvinKL8"

# stage location
STAGE_PATH = f"/scratch-cbe/users/{os.getlogin()}/cache"

MAX_XRDCP = 4  # max number of parallel copies
XRDCP_RETRY = 3  # xrdcp will retry a failed copy

#
LOCAL_EOS_REDIRECTOR = "eos.grid.vbc.ac.at"
GLOBAL_EOS_REDIRECTOR = "xrootd-cms.infn.it"

# Semaphore for XRDCP
sem_xrdcp = asyncio.Semaphore(MAX_XRDCP)


def locate_binary(name: str) -> pathlib.Path:
    """Locate binary in PATH.

    Arguments:
        name: Name of command

    Returns:
        path to binary.

    Raises:
        RuntimeError if not found.
    """
    path = shutil.which(name)
    if path is None:
        raise RuntimeError("Binary {name} not found.")
    return path


# various binaries
DASGOCLIENT = locate_binary("dasgoclient")
XRDCP = locate_binary("xrdcp")
VOMS_PROXY_INIT = locate_binary("voms-proxy-init")
VOMS_PROXY_INFO = locate_binary("voms-proxy-info")


class Datasets:
    "Datasets."
    miniaod: dict[str, str]
    nanoaod: dict[str, str]
    renanoaod: dict[str, str]
    ntuple: dict[str, str]

    def __init__(self, docid: str, period: str) -> None:
        """Init Datasets

        Arguments:
            docid: of Google spreadsheet with datasets definitions
            period: CMS datataking period
        """
        self.miniaod = {}
        self.nanoaod = {}
        self.renanoaod = {}
        self.ntuple = {}

        with urllib.request.urlopen(
            f"https://docs.google.com/spreadsheets/d/{docid}/export?format=csv&sheet={period}"
        ) as response:
            data = response.read().decode("UTF-8")

        for row in csv.DictReader(io.StringIO(data)):
            name = row["Dataset"].strip()
            if not name:
                continue
            val = row["MiniAOD"].strip()
            if val:
                self.miniaod[name] = val
            val = row["NanoAOD"].strip()
            if val:
                self.nanoaod[name] = val
            val = row["ReNanoAOD"].strip()
            if val:
                self.renanoaod[name] = val
            val = row["NTuple"].strip()
            if val:
                self.ntuple[name] = val

    def list_urls(self, dataset: str, tier: str) -> list[str]:
        """List urls of files in the dataset.

        Arguments:
            dataset: Dataset name
            type: dataset type

        Returns:
            list of URLs
        """
        if tier == "MiniAOD":
            return Datasets.get_files_from_das(self.miniaod[dataset])
        elif tier == "NanoAOD":
            return Datasets.get_files_from_das(self.nanoaod[dataset])
        elif tier == "ReNanoAOD":
            return Datasets.get_files_from_fs(
                pathlib.Path("/eos/vbc/experiments/cms", self.renanoaod[dataset][1:])
            )
        elif tier == "NTuple":
            return Datasets.get_files_from_fs(
                pathlib.Path(
                    "/eos/vbc/experiments/cms/store/user/liko/StopsCompressed/nanoTuples",
                    self.ntuple[dataset],
                    "Met",
                    dataset,
                )
            )

    def stage_and_list_urls(self, dataset: str, tier: str, max_files: int) -> list[str]:
        """Stage files of the dataset and return list of paths.

        Arguments:
            dataset: dataset name
            type: dataset type
            max_files: maximum number of files to stage

        Returns:
            list of paths to staged files
        """
        urls = self.list_urls(dataset, tier)
        if max_files:
            urls = urls[:max_files]
        paths = [pathlib.Path(STAGE_PATH, *u.split("/")[5:]) for u in urls]
        asyncio.run(Datasets.stage_all_files(urls, paths))
        return map(str, paths)

    @staticmethod
    async def stage_all_files(urls: list[str], paths: list[str]) -> None:
        """Dispatch files for staging.

        Arguments:
            urls: File urls
            paths: File paths to the stage area
        """
        async with asyncio.TaskGroup() as tg:
            for url, path in zip(urls, paths, strict=True):
                tg.create_task(Datasets.stage_file(url, path))

    @staticmethod
    async def stage_file(url: str, path: pathlib.Path) -> None:
        """Stage a single file.

        Maximum of parallel operations defined by global variable MAX_XRDCP.

        Arguments:
            url : File url
            path: File path in the staging area

        Raises:
            RuntimeError on failed copy operation.
        """
        if path.exists():
            log.debug("Already staged %s ...", url)
            return

        async with sem_xrdcp:
            log.debug("Staging %s ...", url)
            proc = await asyncio.create_subprocess_exec(
                XRDCP,
                "--nopbar",
                "--force",
                "--retry",
                str(XRDCP_RETRY),
                url,
                str(path),
            )
            await proc.wait()

            if proc.returncode != 0:
                log.fatal("Staging %s failed", url)
                raise RuntimeError("XRDCP failed %d", proc.returncode)

    @staticmethod
    def get_files_from_fs(path: pathlib.Path) -> list[str]:
        """Get files of a dataset from the file system.

        Arguments:
            path: path on EOS filesystem

        Returns:
            list of urls
        """
        log.debug("Listing files for %s", path)
        return [
            f"root://{LOCAL_EOS_REDIRECTOR}//{'/'.join(f.parts[5:])}"
            for f in path.glob("**/*.root")
        ]

    @staticmethod
    def get_files_from_das(dasname: str) -> list[str]:
        """Get files of a dataset from CMS DAS.

        As the files might not be on the local URL, the redirector is used.
        Arguments:
            dasname: CMS dataset name

        Returns:
            list of urls
        """
        log.debug("Fetching files from DAS for %s", dasname)
        output = subprocess.run(
            [DASGOCLIENT, "-json", f"-query=file dataset={dasname}"],
            capture_output=True,
        ).stdout
        return [
            f'root://{GLOBAL_EOS_REDIRECTOR}/{x["file"][0]["name"]}'
            for x in json.loads(output)
        ]


@click.group
@click.option(
    "--debug/--no-debug", default=False, help="Enable debug output", show_default=True
)
def main(debug: bool) -> None:
    if debug:
        log.setLevel(logging.DEBUG)


@main.command(name="list")
@click.argument("dataset")
@click.option(
    "--period",
    default="Run2016preVFP",
    type=click.Choice(PERIODS, case_sensitive=False),
    help="CMS datataking period",
    show_default=True,
)
@click.option(
    "--tier",
    default="NTuple",
    type=click.Choice(TYPES, case_sensitive=False),
    help="CMS data tier.",
    show_default=True,
)
def list_files(dataset: str, period: str, tier: str) -> None:
    """List dataset content."""
    datasets = Datasets(GOOGLE_DOCID, period)

    for url in datasets.list_urls(dataset, tier):
        print(url)


@main.command
@click.argument("dataset")
@click.option(
    "--period",
    default="Run2016preVFP",
    type=click.Choice(PERIODS, case_sensitive=False),
    help="CMS datataking period",
    show_default=True,
)
@click.option(
    "--type", default="NTuple", type=click.Choice(TYPES, case_sensitive=False)
)
@click.option("--max-files", default=0, type=click.IntRange(0))
def stage(dataset: str, period: str, tier: str, max_files: int | None) -> None:
    """Stage dataset."""
    datasets = Datasets(GOOGLE_DOCID, period)

    for url in datasets.stage_and_list_urls(dataset, tier, max_files):
        print(url)


if __name__ == "__main__":
    main()
