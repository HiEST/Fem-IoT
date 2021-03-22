from termcolor import colored
import sys


# Available colors: red, green, yellow, blue, magenta, cyan, white.

def prt_warn(*args, **kwargs):
    print(colored(*args,  "yellow"), file=sys.stderr, **kwargs)


def prt_err(*args, **kwargs):
    print(colored(*args,  "red", attrs=["bold"]), file=sys.stderr, **kwargs)


def prt_dbg(*args, **kwargs):
    print(colored(*args,  "magenta"), file=sys.stderr, **kwargs)


def prt_info(*args, **kwargs):
    print(colored(*args,  "white"), **kwargs)


def prt_high(*args, **kwargs):
    print(colored(*args,  "cyan", attrs=["bold"]), **kwargs)


def prt_ok(*args, **kwargs):
    print(colored(*args,  "green"), **kwargs)


# MLFlow

def prt_mlflow_run(run):
    run = run.to_dictionary()
    prt_info(
            (
                " - Run ID: {}\n"
                " - Entry point: {}\n"
                " - Metrics: {}\n"
                " - Params: {}\n"
                " - Artifacts: {}\n"
            ).format(
                run['info']['run_id'],
                run['data']['tags']['mlflow.project.entryPoint'],
                run['data']['metrics'],
                run['data']['params'],
                run['info']['artifact_uri']
                )
            )
