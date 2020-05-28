#!/usr/bin/env python

import os
import sys
import yaml
import time
import logging
import optparse
import pyfiglet
import functools
import multiprocessing
from itertools import repeat
import cerberus.server.server as server
import cerberus.inspect.inspect as inspect
import cerberus.invoke.command as runcommand
import cerberus.kubernetes.client as kubecli
import cerberus.slack.slack_client as slackcli


def smap(f):
    return f()


# Publish the cerberus status
def publish_cerberus_status(status):
    with open('/tmp/cerberus_status', 'w+') as file:
        file.write(str(status))


# Main function
def main(cfg):
    # Start cerberus
    print(pyfiglet.figlet_format("cerberus"))
    logging.info("Starting ceberus")

    # Parse and read the config
    if os.path.isfile(cfg):
        with open(cfg, 'r') as f:
            config = yaml.full_load(f)
        distribution = config["cerberus"].get("distribution", "openshift").lower()
        kubeconfig_path = config["cerberus"].get("kubeconfig_path", "")
        watch_nodes = config["cerberus"].get("watch_nodes", False)
        watch_cluster_operators = config["cerberus"].get("watch_cluster_operators", False)
        watch_namespaces = config["cerberus"].get("watch_namespaces", [])
        cerberus_publish_status = config["cerberus"].get("cerberus_publish_status", False)
        inspect_components = config["cerberus"].get("inspect_components", False)
        slack_integration = config["cerberus"].get("slack_integration", False)
        iterations = config["tunings"].get("iterations", 0)
        sleep_time = config["tunings"].get("sleep_time", 0)
        daemon_mode = config["tunings"].get("daemon_mode", False)
        cores_usage_percentage = config["tunings"].get("cores_usage_percentage", 0.75)

        # Initialize clients
        if not os.path.isfile(kubeconfig_path):
            kubeconfig_path = None
        logging.info("Initializing client to talk to the Kubernetes cluster")
        kubecli.initialize_clients(kubeconfig_path)

        if "openshift-sdn" in watch_namespaces:
            sdn_namespace = kubecli.check_sdn_namespace()
            watch_namespaces = [namespace.replace('openshift-sdn', sdn_namespace)
                                for namespace in watch_namespaces]

        # Cluster info
        logging.info("Fetching cluster info")
        cluster_version = runcommand.invoke("kubectl get clusterversion")
        cluster_info = runcommand.invoke("kubectl cluster-info | awk 'NR==1' | sed -r "
                                         "'s/\x1B\[([0-9]{1,3}(;[0-9]{1,2})?)?[mGK]//g'")  # noqa
        logging.info("\n%s%s" % (cluster_version, cluster_info))

        # Run http server using a separate thread if cerberus is asked
        # to publish the status. It is served by the http server.
        if cerberus_publish_status:
            address = ("0.0.0.0", 8080)
            server_address = address[0]
            port = address[1]
            logging.info("Publishing cerberus status at http://%s:%s"
                         % (server_address, port))
            server.start_server(address)

        # Create slack WebCleint when slack intergation has been enabled
        if slack_integration:
            slack_integration = slackcli.initialize_slack_client()

        # Run inspection only when the distribution is openshift
        if distribution == "openshift" and inspect_components:
            logging.info("Detailed inspection of failed components has been enabled")
            inspect.delete_inspect_directory()

        # Use cluster_info to get the api server url
        api_server_url = cluster_info.split(" ")[-1].strip() + "/healthz"

        # Counter for if api server is not ok
        api_fail_count = 0

        # Variables used for multiprocessing
        pool = multiprocessing.Pool(int(cores_usage_percentage * multiprocessing.cpu_count()))
        manager = multiprocessing.Manager()

        # Initialize the start iteration to 0
        iteration = 0

        # Set the number of iterations to loop to infinity if daemon mode is
        # enabled or else set it to the provided iterations count in the config
        if daemon_mode:
            logging.info("Daemon mode enabled, cerberus will monitor forever")
            logging.info("Ignoring the iterations set")
            iterations = float('inf')
        else:
            iterations = int(iterations)

        # Loop to run the components status checks starts here
        while (int(iteration) < iterations):

            # Initialize a dict to store the operations timings per iteration
            iter_track_time = manager.dict()

            # Capture the start time
            iteration_start_time = time.time()

            iteration += 1

            # Read the config for info when slack integration is enabled
            if slack_integration:
                weekday = runcommand.invoke("date '+%A'")[:-1]
                cop_slack_member_ID = config["cerberus"]["cop_slack_ID"].get(weekday, None)
                slack_team_alias = config["cerberus"].get("slack_team_alias", None)
                slackcli.slack_tagging(cop_slack_member_ID, slack_team_alias)

                if iteration == 1:
                    slackcli.slack_report_cerberus_start(cluster_info, weekday, cop_slack_member_ID)

            # Collect the initial creation_timestamp and restart_count of all the pods in all
            # the namespaces in watch_namespaces
            if iteration == 1:
                pods_tracker = manager.dict()
                pool.starmap(kubecli.namespace_sleep_tracker,
                             zip(watch_namespaces, repeat(pods_tracker)))

            # Execute the functions to check api_server_status, master_schedulable_status,
            # watch_nodes, watch_cluster_operators parallely
            (server_status), (watch_nodes_status, failed_nodes, schedulable_masters), \
                (watch_cluster_operators_status, failed_operators) = \
                pool.map(smap, [functools.partial(kubecli.is_url_available, api_server_url),
                                functools.partial(kubecli.process_nodes, watch_nodes,
                                                  iteration, iter_track_time),
                                functools.partial(kubecli.process_cluster_operator,
                                                  watch_cluster_operators, iteration,
                                                  iter_track_time)])

            # Increment api_fail_count if api server url is not ok
            if not server_status:
                api_fail_count += 1

            # Initialize a shared_memory of type dict to share data between different processes
            failed_pods_components = manager.dict()
            failed_pod_containers = manager.dict()

            # Monitor all the namespaces parallely
            watch_namespaces_start_time = time.time()
            pool.starmap(kubecli.process_namespace, zip(repeat(iteration), watch_namespaces,
                         repeat(failed_pods_components), repeat(failed_pod_containers)))

            watch_namespaces_status = False if failed_pods_components else True
            iter_track_time['watch_namespaces'] = time.time() - watch_namespaces_start_time

            # Check for the number of hits
            if cerberus_publish_status:
                logging.info("HTTP requests served: %s \n"
                             % (server.SimpleHTTPRequestHandler.requests_served))

            if schedulable_masters:
                logging.warning("Iteration %s: Master nodes with incorrect scheduling taint: %s\n"
                                % (iteration, schedulable_masters))

            # Logging the failed components
            if not watch_nodes_status:
                logging.info("Iteration %s: Failed nodes" % (iteration))
                logging.info("%s\n" % (failed_nodes))

            if not watch_cluster_operators_status:
                logging.info("Iteration %s: Failed operators" % (iteration))
                logging.info("%s\n" % (failed_operators))

            if not server_status:
                logging.info("Iteration %s: Api Server is not healthy as reported by %s\n"
                             % (iteration, api_server_url))

            if not watch_namespaces_status:
                logging.info("Iteration %s: Failed pods and components" % (iteration))
                for namespace, failures in failed_pods_components.items():
                    logging.info("%s: %s", namespace, failures)
                    for pod, containers in failed_pod_containers[namespace].items():
                        logging.info("Failed containers in %s: %s", pod, containers)
                logging.info("")

            # Report failures in a slack channel
            if not watch_nodes_status or not watch_namespaces_status or \
                    not watch_cluster_operators_status:
                if slack_integration:
                    slackcli.slack_logging(cluster_info, iteration, watch_nodes_status,
                                           failed_nodes, watch_cluster_operators_status,
                                           failed_operators, watch_namespaces_status,
                                           failed_pods_components)

            # Run inspection only when the distribution is openshift
            if distribution == "openshift" and inspect_components:
                # Collect detailed logs for all the namespaces with failed components parallely
                pool.map(inspect.inspect_component, failed_pods_components.keys())
                logging.info("")
            elif distribution == "kubernetes" and inspect_components:
                logging.info("Skipping the failed components inspection as "
                             "it's specific to OpenShift")

            # Aggregate the status and publish it
            cerberus_status = watch_nodes_status and watch_namespaces_status \
                and watch_cluster_operators_status and server_status

            if cerberus_publish_status:
                publish_cerberus_status(cerberus_status)

            # Sleep for the specified duration
            logging.info("Sleeping for the specified duration: %s\n" % (sleep_time))
            time.sleep(float(sleep_time))

            # Track pod crashes/restarts during the sleep interval in all namespaces parallely
            multiprocessed_output = pool.starmap(kubecli.namespace_sleep_tracker,
                                                 zip(watch_namespaces, repeat(pods_tracker)))

            crashed_restarted_pods = {}
            for item in multiprocessed_output:
                crashed_restarted_pods.update(item)

            if crashed_restarted_pods:
                logging.info("Pods that were crashed/restarted during the sleep interval of "
                             "iteration %s" % (iteration))
                for namespace, pods in crashed_restarted_pods.items():
                    logging.info("%s: %s" % (namespace, pods))
                logging.info("")

            # Capture total time taken by the iteration
            iter_track_time['entire_iteration'] = (time.time() - iteration_start_time) - sleep_time  # noqa

            # Print the captured timing for each operation
            logging.info("-------------------------- Stats of Iteration %s ----------------------"
                         "-----" % (iteration))
            for operation, timing in iter_track_time.items():
                logging.info("Time taken to run %s in iteration %s: %s seconds"
                             % (operation, iteration, timing))
            logging.info("-----------------------------------------------------------------------"
                         "----\n")

        else:
            logging.info("Completed watching for the specified number of iterations: %s"
                         % (iterations))
    else:
        logging.error("Could not find a config at %s, please check" % (cfg))
        sys.exit(1)


if __name__ == "__main__":
    # Initialize the parser to read the config
    parser = optparse.OptionParser()
    parser.add_option(
        "-c", "--config",
        dest="cfg",
        help="config location",
        default="config/config.yaml",
    )
    (options, args) = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("cerberus.report", mode='w'),
            logging.StreamHandler()
        ]
    )
    if (options.cfg is None):
        logging.error("Please check if you have passed the config")
        sys.exit(1)
    else:
        main(options.cfg)
