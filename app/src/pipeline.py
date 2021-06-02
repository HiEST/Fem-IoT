# pylint: disable=no-value-for-parameter
import click
import os
import tempfile
import json


import mlflow
from mlflow.utils import mlflow_tags
from mlflow.entities import RunStatus
from shared.msg_types import (prt_warn, prt_err, prt_ok, prt_high, prt_info,
                              prt_mlflow_run)
from mlflow.tracking.fluent import _get_experiment_id


prt_info(__name__)


def _already_ran(entry_point_name, parameters, git_commit, experiment_id=None):
    """Best-effort detection of if a run with the given entrypoint name,
    parameters, and experiment id already ran. The run must have completed
    successfully and have at least the parameters provided.
    """
    experiment_id = experiment_id if experiment_id is\
        not None else _get_experiment_id()
    client = mlflow.tracking.MlflowClient()
    all_run_infos = client.list_run_infos(experiment_id)
    for run_info in all_run_infos:
        full_run = client.get_run(run_info.run_id)
        tags = full_run.data.tags
        if tags.get(mlflow_tags.MLFLOW_PROJECT_ENTRY_POINT, None)\
                != entry_point_name:
            continue
        match_failed = False
        for param_key, param_value in parameters.items():
            if (type(param_value) is int) or (type(param_value) is float):
                param_value = str(param_value)
            run_value = full_run.data.params.get(param_key)
            # If parameter is an empty string it will come as '', so we need to
            # transform it
            run_value = run_value.strip("'")  # Directly we remove the quotes

            if run_value != param_value:
                match_failed = True
                break
        if match_failed:
            continue

        if run_info.to_proto().status != RunStatus.FINISHED:
            prt_warn(
                    (
                        "Run matched, but is not FINISHED, skipping "
                        "(run_id=%s, status=%s)"
                    ) % (run_info.run_id, run_info.status)
            )
            continue

        previous_version = tags.get(mlflow_tags.MLFLOW_GIT_COMMIT, None)
        if git_commit != previous_version:
            prt_warn(
                    (
                        "Run matched, but has a different git version, skip "
                        "(found=%s, expected=%s)"
                    ) % (previous_version, git_commit)
            )
            continue

        return client.get_run(run_info.run_id)
    prt_err("No matching run has been found.")
    return None


def _get_or_run(entrypoint, parameters, git_commit, use_conda=False,
                use_cache=True):
    if use_cache:
        existing_run = _already_ran(entrypoint, parameters, git_commit)
        if existing_run:
            prt_ok("Found existing run for entrypoint=%s and parameters=%s"
                   % (entrypoint, parameters))
            return existing_run
    prt_info("Launching new run for entrypoint=%s and parameters=%s"
             % (entrypoint, parameters))
    submitted_run = mlflow.run(".", entrypoint, parameters=parameters,
                               use_conda=use_conda)
    return mlflow.tracking.MlflowClient().get_run(submitted_run.run_id)


@click.command()
@click.option("--hdfs", type=str)
@click.option("--ihs-path", type=str)
@click.option("--ihs-hdfs", type=str)
@click.option("--ais-path", type=str)
@click.option("--ais-hdfs", type=str)
@click.option("--emis-hdfs", type=str)
@click.option("--hermes-file", type=str)
@click.option("--step", type=int)
@click.option("--interpolation_lim", type=int)
@click.option("--ae_on_lim", type=int)
@click.option("--unit", type=str)
@click.option("--sfoc", type=str)
@click.option("--model", type=str)
@click.option("--export-db", type=bool)
@click.option("--csv-output", type=str)
def workflow(hdfs, ihs_path, ihs_hdfs, ais_path, ais_hdfs, emis_hdfs, hermes_file,
             step, interpolation_lim, ae_on_lim, unit, sfoc, model, export_db,
             csv_output):
    # Note: The entrypoint names are defined in MLproject.
    # The artifact directories are documented by each step's .py file.
    with mlflow.start_run() as active_run:
        mlflow.set_tag("mlflow.runName", 'pipeline')

        git_commit = active_run.data.tags.get(mlflow_tags.MLFLOW_GIT_COMMIT)

        log_run_id = dict()

        # Load IHS
        prt_high("Load IHS")
        load_metadata_param = {
                "hdfs": hdfs,
                "ihs_path": ihs_path,
                "ihs_hdfs": ihs_hdfs
                }
        load_metadata = _get_or_run(
                "load_metadata", load_metadata_param, git_commit)
        prt_ok("Load metadata done.")
        prt_mlflow_run(load_metadata)
        log_run_id["run_id_load_metadata"] = load_metadata.info.run_id

        # Load AIS
        prt_high("Load AIS")
        ingest_csv_param = {
                "hdfs": hdfs,
                "ais_path": ais_path,
                "ais_hdfs": ais_hdfs
                }
        ingest_csv = _get_or_run("ingest_csv", ingest_csv_param, git_commit)
        prt_ok("Ingest CSV done.")
        prt_mlflow_run(ingest_csv)
        log_run_id["run_id_ingest_csv"] = ingest_csv.info.run_id

        # If we didn't specify a URI of the HDFS input data, we obtain it from
        # the archived ones
        if not ihs_hdfs:
            ihs_hdfs = os.path.join(
                    load_metadata.info.artifact_uri, "ihs_data.parquet")
        if not ais_hdfs:
            ais_hdfs = os.path.join(
                    ingest_csv.info.artifact_uri, "ais_data.parquet")

        #  Store AIS and IHS in DB

        if export_db:
            # Export IHS
            prt_high("Exporting IHS to DB")
            log_run_id["run_id_export_db"] = dict()
            ihs_table = "ihs"
            export_db_param = {
                "hdfs": hdfs,
                "input_data": ihs_hdfs,
                "table": ihs_table,
                "table_type": "ihs"
            }
            export_db_ihs = _get_or_run(
                    "export_postgis", export_db_param, git_commit)
            log_run_id["run_id_export_db"]["ihs"] = export_db_ihs.info.run_id

            # Export AIS
            prt_high("Exporting AIS to DB")
            export_db_param = {
                "hdfs": hdfs,
                "input_data": ais_hdfs,
                "table": "ais",
                "idx_fields": "(nombre, imo, typeofshipandcargo)",
                "table_type": "ais",
                "ihs_table": ihs_table
            }
            export_db_ais = _get_or_run(
                    "export_postgis", export_db_param, git_commit)
            log_run_id["run_id_export_db"]["ais"] = export_db_ais.info.run_id

            # Create ship_info view

        # Compute Emissions

        prt_high("Compute Emissions")
        compute_emissions = list()
        model = model.split(",")
        log_run_id["run_id_compute_emissions"] = dict()
        for m in model:
            prt_high("Computing model: {}\n".format(m))
            compute_emissions_param = {
                    "hdfs": hdfs,
                    "ihs_hdfs": ihs_hdfs, "ais_hdfs": ais_hdfs,
                    "model": m, "step": step,
                    "interpolation_lim": interpolation_lim, "unit": unit,
                    "ae_on_lim": ae_on_lim,  "sfoc": sfoc
            }
            compute_emissions.append(_get_or_run(
                    "compute_emissions", compute_emissions_param, git_commit))

        prt_ok("Compute emissions done.")
        for (m, run) in zip(model, compute_emissions):
            prt_info("Model: {}".format(m))
            prt_mlflow_run(run)
            log_run_id["run_id_compute_emissions"][m] = run.info.run_id

        # Export to DB
        if export_db:
            export_db_l = list()
            for (emis, m) in zip(compute_emissions, model):
                prt_high("Saving results in DB: {}\n".format(m))
                emis_parquet_uri = os.path.join(
                    emis.info.artifact_uri, "emissions.parquet")
                run_id = emis.info.run_id
                export_db_param = {
                    "hdfs": hdfs,
                    "input_data": emis_parquet_uri,
                    "table": "emis_"+m+"_"+run_id
                }
                export_db_l.append(_get_or_run(
                        "export_postgis", export_db_param, git_commit))
            prt_ok("Export DB done.")
            for (m, run) in zip(model, export_db_l):
                prt_info("Model: {}".format(m))
                prt_mlflow_run(run)
                log_run_id["run_id_export_db"][m] = run.info.run_id

        # Export to csv
        if csv_output:  # Path exists, i.e. not ""
            export_csv_l = list()
            for (emis, m) in zip(compute_emissions, model):
                prt_high("Saving results in CSV: {}\n".format(m))
                emis_parquet_uri = os.path.join(
                    emis.info.artifact_uri, "emissions.parquet")
                run_id = emis.info.run_id
                export_csv_param = {
                    "hdfs": hdfs,
                    "input_file": emis_parquet_uri,
                    "output_file": csv_output+"/emis_"+m+"_"+run_id+".csv"
                }
                export_csv_l.append(_get_or_run(
                        "export_csv", export_csv_param, git_commit))
            log_run_id["run_id_export_csv"] = dict()
            for (m, run) in zip(model, export_csv_l):
                prt_info("Model: {}".format(m))
                prt_mlflow_run(run)
                log_run_id["run_id_export_csv"][m] = run.info.run_id

        # Compare emis HERMES
        if hermes_file:  # If there is a file specified
            compare_hermes = list()
            log_run_id["run_id_compare_hermes"] = dict()
            for (emis, m) in zip(compute_emissions, model):
                prt_high("Comparing with HERMES: {}\n".format(m))
                emis_parquet_uri = os.path.join(
                    emis.info.artifact_uri, "emissions.parquet")
                run_id = emis.info.run_id
                compare_hermes_param = {
                    "hdfs": hdfs,
                    "input_file": emis_parquet_uri,
                    "hermes_file": hermes_file,
                    "model": m
                }
                compare_hermes.append(_get_or_run(
                        "compare_hermes", compare_hermes_param, git_commit))

            prt_ok("Comparing with HERMES done.")
            for (m, run) in zip(model, compare_hermes):
                prt_info("Model: {}".format(m))
                prt_mlflow_run(run)
                log_run_id["run_id_compare_hermes"][m] = run.info.run_id

        # Export run ID json
        log_pipe_runs = tempfile.mkdtemp() + '/pipe_run_ids.json'
        with open(log_pipe_runs, mode='w') as f:
            json.dump(log_run_id, f)
            f.seek(0)
            mlflow.log_artifact(log_pipe_runs)


if __name__ == "__main__":
    workflow()
