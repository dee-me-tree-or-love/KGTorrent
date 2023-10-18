#!/usr/bin/env python3

import argparse
import sys
import time
from pathlib import Path

# FIXME(remove-after-testing):
# from unittest.mock import create_autospec

import KGTorrent.config as config
from KGTorrent.data_loader import DataLoader
from KGTorrent.db_communication_handler import DbCommunicationHandler
from KGTorrent.downloader import Downloader, DownloadStrategies
from KGTorrent.mk_preprocessor import MkPreprocessor


class SimpleDependencyProvider:
    DB_ENGINE = DbCommunicationHandler
    MK_PREPROCESSOR = MkPreprocessor
    DOWNLOADER = Downloader
    # FIXME(remove-after-testing):
    # DB_ENGINE = create_autospec(DbCommunicationHandler)
    # MK_PREPROCESSOR = create_autospec(MkPreprocessor)
    # DOWNLOADER = create_autospec(Downloader)


class Commands:
    INIT = "init"
    REFRESH = "refresh"
    DOWNLOAD = "download"

    @classmethod
    def commands(cls) -> [str]:
        return [cls.INIT, cls.REFRESH, cls.DOWNLOAD]


print(Commands.commands())


def main():
    """Entry-point function for KGTorrent.
    It orchestrates function/method calls to build and populate the KGTorrent database and dataset.
    """

    # Create the parser
    my_parser = argparse.ArgumentParser(
        prog="KGTorrent",
        usage="%(prog)s <" + "|".join(Commands.commands()) + "> [options]",
        description="Initialize or refresh KGTorrent",
    )

    # Add the arguments
    my_parser.add_argument(
        "command",
        type=str,
        choices=[Commands.INIT, Commands.REFRESH, Commands.DOWNLOAD],
        help=f"Use the `{Commands.INIT}` command to create KGTorrent from scratch or "
        + f"the `{Commands.REFRESH}` command to update KGTorrent "
        + "according to the last version of Meta Kaggle. "
        + f"Use the `{Commands.REFRESH}` command to only download the notebooks "
        + "for an already initialized KGTorrent.",
    )

    my_parser.add_argument(
        "-s",
        "--strategy",
        type=str,
        choices=list(DownloadStrategies.strategies()) + ["SKIP"],
        default="HTTP",
        help=f"Use the `{DownloadStrategies.API}` strategy to download Kaggle kernels via the Kaggle's official API; "
        + f"Use the `{DownloadStrategies.HTTP}` strategy to download full kernels via HTTP requests. "
        + "N.B.: Notebooks downloaded via the Kaggle API miss code cell outputs. "
        + "Use the `SKIP` strategy to skip the download step completely.",
    )

    my_parser.add_argument(
        "-m",
        "--matching",
        type=str,
        action="extend",
        nargs="+",
        default=[],
        help="Provide a comma-separated list of notebook identifiers for download. "
        + "If provided, only matching notebooks in dataset are downloaded. "
        + "Otherwise KGTorrent downloads all notebooks.",
    )

    # Execute the parse_args() method
    args = my_parser.parse_args()

    command = args.command

    print("************************")
    print("*** KGTORRENT STARTED***")
    print("************************")

    # Create db engine
    print(f"## Connecting to {config.db_name} db on port {config.db_port} as user {config.db_username}")
    db_engine = SimpleDependencyProvider.DB_ENGINE(
        config.db_username,
        config.db_password,
        config.db_host,
        config.db_port,
        config.db_name,
    )

    print("## Connection with database established.")

    # CHECK USER VARIABLES
    proceed: bool = False

    # Check db emptiness
    if db_engine.db_exists():
        if command == Commands.INIT:
            print(f"Database {config.db_name} already exists. ", file=sys.stderr)
            print(f"Please, provide a name that is not already in use for the KGTorrent database.", file=sys.stderr)
            proceed = False
        if command == Commands.REFRESH:
            print(f"Database {config.db_name} already exists. This operation will reinitialize the current database")
            print("and populate it with the provided MetaKaggle version.")
            ans = input(f"Are you sure to re-initialize {config.db_name} database? [yes]\n")
            if ans.lower() == "yes":
                proceed = True
            else:
                proceed = False
        if command == Commands.DOWNLOAD:
            print(f"Database {config.db_name} already exists. This operation will only download the notebooks.")

    else:
        proceed = True

    # Check download folder emptiness when init
    data = next(Path(config.nb_archive_path).iterdir(), None)
    if (data is not None) and (command == Commands.INIT or command == Commands.DOWNLOAD):
        print(f"Download folder {config.nb_archive_path} is not empty.", file=sys.stderr)
        print("Please, provide the path to an empty folder to store downloaded notebooks.", file=sys.stderr)
        proceed = False

    # KGTorrent data preparation process
    if proceed and (command != Commands.DOWNLOAD):
        print("********************")
        print("*** LOADING DATA ***")
        print("********************")
        dl = DataLoader(config.constraints_file_path, config.meta_kaggle_path)

        print("***********************************")
        print("** TABLES PRE-PROCESSING STARTED **")
        print("***********************************")
        mk = SimpleDependencyProvider.MK_PREPROCESSOR(dl.get_tables_dict(), dl.get_constraints_df())
        processed_dict, stats = mk.preprocess_mk()

        print("*************")
        print("*** STATS ***")
        print("*************\n")
        print(stats)

        print("## Initializing DB...")
        db_engine.create_new_db(drop_if_exists=True)

        print("***************************")
        print("** DB POPULATION STARTED **")
        print("***************************")
        db_engine.write_tables(processed_dict)

        print("** APPLICATION OF CONSTRAINTS **")
        db_engine.set_foreign_keys(dl.get_constraints_df())

        # Free memory
        del dl
        del mk

    # KGTorrent notebook download process
    if proceed:
        print("** QUERYING KERNELS TO DOWNLOAD **")
        nb_identifiers = db_engine.get_nb_identifiers(config.nb_conf["languages"])

        print("*******************")
        print("*** Identifiers ***")
        print("*******************\n")
        print(nb_identifiers)

        del db_engine

        # Download the notebooks and update the db with their local path
        # To get a specific subset of notebooks, query the database by using
        # the db_schema object as needed.
        print("*******************************")
        print("** NOTEBOOK DOWNLOAD STARTED **")
        print("*******************************")

        print(f"# Selected strategy. {args.strategy}")
        if not args.strategy == "SKIP":
            download_identifiers = list(set(nb_identifiers) & set(args.matching))
            downloader = SimpleDependencyProvider.DOWNLOADER(download_identifiers, config.nb_archive_path)
            downloader.download_notebooks(strategy=args.strategy)
            print("## Download finished.")
        else:
            print("## Download skipped.")

    time.sleep(0.2)
    print("## KGTorrent end")


def entry():
    start_time = time.time()
    main()
    print("--- %s minutes ---" % ((time.time() - start_time) / 60))


if __name__ == "__main__":
    entry()
