#!/usr/env/bin python

#  Author: Yarin Galmor

from task_common_methods import configure_logger, get_ssh_client
from devops.deployment.simulation.config.sim_config import sim_config
import subprocess
from enum import IntEnum
import abc
import jenkins
import time
from distutils.spawn import find_executable
import os
import shutil


class TaskStatus(IntEnum):
    """
    TaskStatus represents flow status of a single task
    """

    TIMEOUT_EXCEEDED = -3  # Task execution stopped because it passed its timeout.
    CANCELED = -2  # Task was canceled during deployment since one of it dependencies has failed.
    FAILED = -1  # Task execution or validation was failed.
    INITIALIZED = 0  # Task has been initialized but not yet being executed.
    WAITING = 1  # Task is being executed and waiting for dependencies to be over.
    RUNNING = 2  # Task is being executed and is currently running.
    FINISHED = 3  # Task is finished to be executed and is before validation.
    DONE = 4  # Task was finished successfully.
    SKIPPED = 5  # Task is initialized but will not be executed.
    IGNORE_ERRORS = 6  # Task execution failed but task was set for ignoring it from errors.


class Task(object):
    """
    Task class. Generic class for deploying any kind of execution method as part of a major procedure.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, execute=None, timeout=60, retries=0, delay=5, configuration=None, dependencies=None,
                 sub_tasks=None, skip_task=False, ignore_task_errors=False):
        """
        Task Constructor
        :param name: task name
        :param execute: command for execution.
        :param timeout: task timeout, default is 60 seconds.
        :param retries: Number of execution retries after failure.
        :param delay: Delay between retries in seconds.
        :param configuration:
        :param dependencies: set consists Tasks which it's depended on.
        :param sub_tasks: set consists Tasks are set as it's sub tasks.
        """

        self._execute = execute
        self._timeout = timeout
        self._starting_time = None
        self._retries = retries
        self._delay = delay
        self.configuration = configuration
        if dependencies:
            self._dependencies = dependencies
            self.blocking_tasks = dependencies
        else:
            self._dependencies = set()
            self.blocking_tasks = set()

        self.name = name
        self.status = TaskStatus.INITIALIZED
        self._status_history = [TaskStatus.INITIALIZED]  # for UT, verify rotation of task statuses.
        self.retries_left = retries
        self.output = ""
        self.duration = 0
        self.logger = configure_logger(name.replace(" ", "_"))

        if sub_tasks:
            self._sub_tasks = sub_tasks
        else:
            self._sub_tasks = set()

        self.skip_task = skip_task
        self.ignore_task_errors = ignore_task_errors

    def get_sub_tasks(self):
        return self._sub_tasks

    def get_dependencies(self):
        return self._dependencies

    def get_execute(self):
        return self._execute

    def get_timeout(self):
        return self._timeout

    def get_starting_time(self):
        return self._starting_time

    def get_retries(self):
        return self._retries

    def get_delay(self):
        """ delay between retries"""
        return self._delay

    def set_task_status(self, task_status):
        self.status = task_status
        self.update_status_history()

    def promote_task_status(self):
        self.status = TaskStatus(self.status + 1)
        self.update_status_history()

    def update_status_history(self):
        self._status_history.append(self.status)

    def get_status_history(self):
        return self._status_history

    def set_task_output(self, output):
        self.output = output

    def write_output_to_file(self, file_path):
        with open(file_path, 'w') as log_file:
            log_file.write(self.output)

    def set_task_duration(self, duration):
        self.duration = duration

    def execute_shell_task(self, proc_extra_envs={}):
        for command in self._execute:
            proc_env = os.environ.copy()
            proc_env.update(proc_extra_envs)
            try:
                output = subprocess.check_output(command, shell=True, env=proc_env)
                stdout = output.split("\n")
                for line in stdout:
                    self.logger.info("%s - line - %s" % (self.name, line))
                    self.set_task_output("%s\n%s" % (self.output, line))

            except subprocess.CalledProcessError as e:
                raise RuntimeError("%s - Command %s returned with non-zero return code %s.\n"
                                   "Output - \n%s." % (self.name, command, e.returncode, e.output))

        self.promote_task_status()

    def set_task_retries_left(self, retries):
        self.retries_left = retries

    def add_task_dependencies(self, dependencies):
        self._dependencies = self._dependencies.union(dependencies)

    def add_blocking_tasks(self, tasks):
        self.blocking_tasks = self.blocking_tasks.union(tasks)

    def cancel(self):
        self.logger.info("task name %s - task is Canceled" % self.name)
        self.set_task_status(TaskStatus.CANCELED)

    def increment_duration(self):
        self.set_task_duration(int(time.time() - self._starting_time))

    def set_starting_time(self, starting_time):
        self._starting_time = starting_time

    def validate_execute(self):
        if not isinstance(self._execute, list):
            raise ValueError("Execute expected to be a list of strings commands, got %s" % self._execute)
        else:
            for command in self._execute:
                if not isinstance(command, str):
                    raise ValueError("Execute expected to be a list of strings commands, got %s" % command)

    def pre_task(self):
        pass

    def execute_task(self):
        """ In case no executable is defined, promoting task status to Finished"""
        self.set_task_status(TaskStatus.FINISHED)

    def post_task(self):
        pass

    def initiate_sub_tasks(self):
        pass

    def validate_parameters(self):
        pass

    @abc.abstractmethod
    def validator(self):
        pass


class BashTask(Task):
    def __init__(self, name, execute=None, timeout=60, retries=0, delay=5, configuration=None, dependencies=None,
                 sub_tasks=None, skip_task=False, ignore_task_errors=False):
        super(BashTask, self).__init__(name=name, execute=execute, timeout=timeout,
                                       retries=retries, delay=delay, configuration=configuration,
                                       dependencies=dependencies, sub_tasks=sub_tasks, skip_task=skip_task,
                                       ignore_task_errors=ignore_task_errors)

    def validate_parameters(self):
        self.validate_execute()

    def execute_task(self):
        self.execute_shell_task()

    def pre_task(self):
        pass

    def post_task(self):
        pass

    @abc.abstractmethod
    def validator(self):
        raise NotImplementedError("Please Implement this method")


class SshTask(Task):
    def __init__(self, name, execute=None, timeout=60, retries=0, delay=5, configuration=None, dependencies=None,
                 sub_tasks=None, remote_host=None, remote_user="root", remote_password=None, ssh_port=22,
                 ssh_args=None, skip_task=False, ignore_task_errors=False):
        super(SshTask, self).__init__(name=name, execute=execute, timeout=timeout, retries=retries, delay=delay,
                                      configuration=configuration, dependencies=dependencies, sub_tasks=sub_tasks,
                                      skip_task=skip_task, ignore_task_errors=ignore_task_errors)

        self._remote_host = remote_host
        self._remote_user = remote_user
        self._remote_password = remote_password
        self._ssh_port = ssh_port
        self._ssh_args = "-o StrictHostKeyChecking=no"
        if ssh_args:
            self._ssh_args += " %s" % ssh_args

    def validate_parameters(self):
        """
        Validate ssh essential params - remote host, remote_password
        :return:
        """
        self.validate_execute()

        if not self._remote_host:
            raise ValueError("Remote host is not configured for SshTask.")

        if not self._remote_password:
            raise ValueError("Remote password is not configured for user %s." % self._remote_user)

        try:
            get_ssh_client(self._remote_host, self._remote_user, self._remote_password)

        except Exception as e:
            raise ValueError("Could not create ssh client to remote host - %s.\n%s" % (self._remote_host,
                                                                                       e))

    def execute_task(self):
        ssh_client = get_ssh_client(self._remote_host, username=self._remote_user, password=self._remote_password,
                                    port=self._ssh_port)

        for command in self._execute:
            self.logger.info("Executing command - %s, on host %s" % (command, self._remote_host))
            stdin, stdout, stderr = ssh_client.exec_command(command)
            stderr_output = stderr.read()
            stdout_output = stdout.read()
            return_code = stdout.channel.recv_exit_status()

            self.logger.info("Command ended with return code - %s.\n"
                             "STDOUT:\n%s\nSTDERR:\n%s" % (return_code, stdout_output, stderr_output))

            if return_code:
                raise RuntimeError()

        self.promote_task_status()

    def pre_task(self):
        pass

    def post_task(self):
        pass

    @abc.abstractmethod
    def validator(self):
        raise NotImplementedError("Please Implement this method")


class JenkinsTask(Task):
    def __init__(self, name, timeout=60, retries=0, delay=5, configuration=None, dependencies=None,
                 sub_tasks=None, jenkins_job_name=None, jenkins_job_params=None, jenkins_user=None,
                 jenkins_password=None, jenkins_url=None, skip_task=False, ignore_task_errors=False):
        super(JenkinsTask, self).__init__(name=name, timeout=timeout, retries=retries, delay=delay,
                                          configuration=configuration, dependencies=dependencies, sub_tasks=sub_tasks,
                                          skip_task=skip_task, ignore_task_errors=ignore_task_errors)
        self._jenkins_job_name = jenkins_job_name
        self._jenkins_job_params = jenkins_job_params
        self._jenkins_user = jenkins_user
        self._jenkins_password = jenkins_password

        if jenkins_url:
            self._jenkins_url = jenkins_url
        else:
            self._jenkins_url = os.getenv("JENKINS_URL")

        self._jenkins_server = jenkins.Jenkins(self._jenkins_url, self._jenkins_user, self._jenkins_password)
        self._jenkins_build_number = None
        self._jenkins_job = None
        self._jenkins_build_url = None

    def validate_parameters(self):
        if not isinstance(self._jenkins_job_name, str):
            raise ValueError("Expected string name of jenkins job name, got %s" % self._jenkins_job_name)

        if not isinstance(self._jenkins_job_params, dict):
            raise ValueError("Expected dict with jenkins job parameters, got %s" % self._jenkins_job_params)

        if not self._jenkins_server:
            raise ValueError("Failed to initiate jenkins_server instance")

        try:
            job_name = self._jenkins_server.get_job_name(self._jenkins_job_name)
            if not job_name:
                raise ValueError("job name doesn't exist, got %s" % self._jenkins_job_name)

        except jenkins.JenkinsException as e:
            raise ValueError("Jenkins credentials are invalid.\n%s" % e)

    def _get_relevant_build_id(self):
        self.logger.info("Looking for relevant build id")
        jenkins_job = self._jenkins_server.get_job_info(self._jenkins_job_name)
        last_build_number = jenkins_job["lastBuild"]["number"]
        self.logger.info("Start looking from build id - %s" % last_build_number)
        self.logger.info("Marked as build id - %s" % self._jenkins_build_number)
        look_backwards_to_build = 3  # Go 5 build back for looking to relevant build
        build_parameters = {}

        while last_build_number < self._jenkins_build_number:
            jenkins_job = self._jenkins_server.get_job_info(self._jenkins_job_name)
            last_build_number = jenkins_job["lastBuild"]["number"]

        for build_id in range(last_build_number, last_build_number - look_backwards_to_build, -1):
            if build_id == self._jenkins_build_number:
                self.logger.info("Build id match")
                build_found = True
                build = self._jenkins_server.get_build_info(self._jenkins_job_name, build_id)
                for action in build['actions']:
                    if '_class' in action:
                        if action['_class'] == "hudson.model.ParametersAction":
                            build_parameters = build['actions'][0]['parameters']

                if build_parameters:
                    for param in self._jenkins_job_params:
                        for build_param in build_parameters:
                            if param == build_param["name"]:
                                if self._jenkins_job_params[param] != build_param["value"]:
                                    self.logger.info("Param %s does not match. Orig value %s, "
                                                     "Current value %s " % (param, self._jenkins_job_params[param],
                                                                            build_param["value"]))
                                    build_found = False

                if build_found:
                    self._jenkins_build_url = "%s/job/%s/%s/consoleFull" % (self._jenkins_url,
                                                                            self._jenkins_job_name,
                                                                            self._jenkins_build_number)

                    self.logger.info("Watching build - %s" % self._jenkins_build_url)
                    return build_id
                else:
                    self.logger.info("Build id %s is not the relevant build to watch." % build_id)

        raise RuntimeError("Could not find relevant build id.")

    def _watch_jenkins_build(self):
        is_build_running = False
        build_starting_timeout = 60  # How much time to wait, in seconds, until a build starts.
        start_watching_time = time.time()
        time_since_build_triggered = 0

        build_id = self._get_relevant_build_id()

        while not is_build_running and time_since_build_triggered < build_starting_timeout:
            # Wait for build to start running. Waiting timeout is set to 60 seconds.
            build = self._jenkins_server.get_build_info(self._jenkins_job_name, build_id)

            if "building" in build:
                is_build_running = build['building']
            else:
                raise RuntimeError("Build dict has bad format - %s" % build)

            time_since_build_triggered = int(time.time() - start_watching_time)

        self.logger.info("Watching %s with build ID %s" % (self._jenkins_job_name, self._jenkins_build_number))
        self.logger.info("Build link - %s" % self._jenkins_build_url)

        while is_build_running:
            build = self._jenkins_server.get_build_info(self._jenkins_job_name, self._jenkins_build_number)
            if not build["building"]:
                is_build_running = False

        self.logger.info("Build %s has finished" % self._jenkins_job_name)

        if "result" in build:
            if "result" in build:
                if build["result"] != "SUCCESS":
                    self.set_task_status(TaskStatus.FAILED)
                    self.logger.error("Task %s failed. Refer to the following "
                                      "link for details - %s" % (self.name, self._jenkins_build_url))
            else:
                raise ValueError("Bad jenkins build structure. missing value of result.")
        else:
            raise RuntimeError("Build dict has bad format - %s" % build)

    def execute_task(self):
        jenkins_job = self._jenkins_server.get_job_info(self._jenkins_job_name)
        self._jenkins_build_number = jenkins_job["nextBuildNumber"]
        self._jenkins_server.build_job(self._jenkins_job_name, self._jenkins_job_params)
        self._watch_jenkins_build()

        self.promote_task_status()

    def pre_task(self):
        pass

    def post_task(self):
        pass

    @abc.abstractmethod
    def validator(self):
        raise NotImplementedError("Please Implement this method")


class AnsiblePlaybookTask(Task):
    ANSIBLE_EXECUTE_BIN = "/usr/bin/ansible-playbook"

    def __init__(self, name, timeout=60, retries=0, delay=5, configuration=None, dependencies=None,
                 sub_tasks=None, ansible_playbook=None, ansible_inventory_file=None, ansible_remote_hosts=None,
                 ansible_extra_args=None, ansible_user="root", ansible_password=None, ansible_use_password=False,
                 ansible_key_file=None, ansible_use_key_file=False, skip_task=False, ignore_task_errors=False):
        super(AnsiblePlaybookTask, self).__init__(name=name, timeout=timeout, retries=retries, delay=delay,
                                                  configuration=configuration, dependencies=dependencies,
                                                  sub_tasks=sub_tasks, skip_task=skip_task,
                                                  ignore_task_errors=ignore_task_errors)

        self._ansible_playbook = ansible_playbook
        self._ansible_inventory_file = ansible_inventory_file
        self._ansible_remote_hosts = ansible_remote_hosts
        self._ansible_extra_args = ""
        if ansible_extra_args:
            self._ansible_extra_args += " %s" % ansible_extra_args
        self._ansible_user = ansible_user
        self._ansible_password = ansible_password
        self._ansible_use_password = ansible_use_password
        self._ansible_key_file = ansible_key_file
        self._ansible_use_key_file = ansible_use_key_file

    def validate_parameters(self):
        """
        Validate essential args for executing AnsiblePlaybookTask - ansible_playbook, ansible_user, and ansible_password
        """
        is_valid_param = True

        if not self._ansible_playbook:
            self.logger.error("Ansible playbook is not configured for ansible-playbook execute method.")
            is_valid_param = False

        if not self._ansible_user:
            self.logger.error("Ansible user is not configured for ansible-playbook execute method.")
            is_valid_param = False

        if not self._ansible_password and self._ansible_use_password:
            self.logger.error("Ansible password is not configured for ansible-playbook execute method.")
            is_valid_param = False

        if not self._ansible_key_file and self._ansible_use_key_file:
            self.logger.error("Ansible ssh key file is not configured for ansible-playbook execute method.")
            is_valid_param = False

        if not self._ansible_use_key_file ^ self._ansible_use_password:
            self.logger.error("One of ansible_use_key_file or ansible_ansible_use_password should be set with True."
                              "Got ansible_use_key_file %s, ansible_use_password %s" % (self._ansible_use_key_file,
                                                                                        self._ansible_use_password))
            is_valid_param = False

        if not is_valid_param:
            self.set_task_status(TaskStatus.FAILED)
            raise ValueError("Ansible params are not configured properly.")

        if not os.path.isfile(self._ansible_playbook):
            raise ValueError("Ansible playbook file doesn't exist")

    def execute_task(self):
        """
        Verify that ansible has all its
        :return:
        """
        if self._ansible_use_password:
            self._ansible_extra_args += " ansible_password=%s" % self._ansible_password

        command = "%s " % self.ANSIBLE_EXECUTE_BIN

        if self._ansible_inventory_file:
            if not os.path.isfile(self._ansible_inventory_file):
                raise ValueError("Ansible inventory file doesn't exist")
            command += "-i %s " % self._ansible_inventory_file

        elif self._ansible_remote_hosts:
            command += "-i '%s, ' " % self._ansible_remote_hosts

        if self._ansible_use_key_file:
            if not os.path.isfile(self._ansible_key_file):
                raise ValueError("Ansible key file doesn't exist")
            command += "--key-file %s " % self._ansible_key_file

        command += "-u %s -e \'"

        for arg in self._ansible_extra_args:
            command += "%s: %s, " % (arg, self._ansible_extra_args[arg])

        command = command[:-2] + "\' %s" % self._ansible_playbook

        self._execute = [command]

        self.logger.info("Execute - %s" % self._execute)

        self.execute_shell_task(proc_extra_envs={"ANSIBLE_HOST_KEY_CHECKING": "false"})

        self.promote_task_status()

    def pre_task(self):
        pass

    def post_task(self):
        pass

    @abc.abstractmethod
    def validator(self):
        raise NotImplementedError("Please Implement this method")


class PythonTask(Task):
    def __init__(self, name, execute=None, timeout=60, retries=0, delay=5, configuration=None, dependencies=None,
                 sub_tasks=None, skip_task=False, ignore_task_errors=False):
        super(PythonTask, self).__init__(name=name, execute=execute, timeout=timeout,
                                         retries=retries, delay=delay, configuration=configuration,
                                         dependencies=dependencies, sub_tasks=sub_tasks, skip_task=skip_task,
                                         ignore_task_errors=ignore_task_errors)

    def validate_parameters(self):
        if not callable(self._execute):
            raise ValueError("Expected method but got %s" % type(self._execute))

    def execute_task(self):
        self._execute()
        self.promote_task_status()

    def pre_task(self):
        pass

    def post_task(self):
        pass

    @abc.abstractmethod
    def validator(self):
        raise NotImplementedError("Please Implement this method")


class TerraformTask(Task):
    TERRAFORM_SUPPORTED_COMMANDS = ["apply", "destroy"]

    def __init__(self, name, command=None, terraform_conf_file=None, terraform_vars=None, terraform_var_file=None,
                 timeout=60, retries=0, delay=5, configuration=None, dependencies=None, sub_tasks=None, skip_task=False,
                 ignore_task_errors=False):
        """
        Terraform Task constructor
        @param name: Task's name
        @param command: Terraform command. Should be listed under TERRAFORM_SUPPORTED_COMMANDS
        @param terraform_conf_file: Relative path to the conf file - devops/deployment..
        @param terraform_vars: Terraform vars dict oriented.
        @param terraform_var_file: Relative path to the var file - devops/deployment..
        @param timeout: Task timeout
        @param retries: Task retries in case of failure.
        @param delay: Task delay between retries of re-execute the task.
        @param configuration: Task's configuration
        @param dependencies: Task's dependencies
        @param sub_tasks: Task's sub_tasks
        """

        super(TerraformTask, self).__init__(name=name, timeout=timeout, retries=retries, delay=delay,
                                            configuration=configuration, dependencies=dependencies, sub_tasks=sub_tasks,
                                            skip_task=skip_task, ignore_task_errors=ignore_task_errors)

        self._command = command
        self._terraform_conf_file = terraform_conf_file
        self._terraform_vars = terraform_vars
        self._terraform_var_file = terraform_var_file

    def validate_parameters(self):
        if not find_executable("terraform"):
            raise RuntimeError("terraform command is missing on this host.")

        if self._command not in self.TERRAFORM_SUPPORTED_COMMANDS:
            raise ValueError("Got terraform command '%s', which is not supported. "
                             "Please refer to a supported command - %s" % (self._command,
                                                                           self.TERRAFORM_SUPPORTED_COMMANDS))

        if self._terraform_conf_file is not None:
            if not os.path.isfile("%s" % self._terraform_conf_file):
                raise ValueError("Terraform conf file could not be found in terrafrom directory. "
                                 "Expected devops/deployment...")

            elif not (self._terraform_conf_file.endswith('.tf') or self._terraform_conf_file.endswith('.tf.json')):
                raise ValueError("Terraform conf file is bad formatted or filename is not with the right extension -"
                                 " '.tf'/'.tf.json' - %s" % self._terraform_conf_file)

        if self._terraform_vars is not None:
            if not isinstance(self._terraform_vars, dict):
                raise ValueError("Expected to get a dict for terraform_vars, but got %s" % type(self._terraform_vars))

        if self._terraform_var_file is not None:
            if not os.path.isfile("%s" % self._terraform_var_file):
                raise ValueError("Terraform var file could not be found in terrafrom directory. "
                                 "Expected devops/deployment...")

            elif not (self._terraform_var_file.endswith('.tfvars')):
                raise ValueError("Terraform var file is bad formatted or filename is not with the right extension -"
                                 " '.tfvars' - %s" % self._terraform_var_file)

    def execute_task(self):
        working_dir = os.getcwd()
        if not os.path.isdir("./terraform_task"):
            os.mkdir("terraform_task")

        terraform_conf_filename = os.path.basename(self._terraform_conf_file)
        shutil.copyfile(self._terraform_conf_file, "terraform_task/%s" % terraform_conf_filename)

        os.chdir("terraform_task")

        terraform_bin_command = "terraform"

        terraform_commnand_args = ""

        if self._terraform_var_file:
            terraform_var_filename = os.path.basename(self._terraform_var_file)
            shutil.copyfile(self._terraform_var_file, "terraform_task/%s" % terraform_var_filename)
            terraform_commnand_args += "-var-file %s " % terraform_var_filename

        if self._terraform_vars:
            for var in self._terraform_vars:
                terraform_commnand_args += "-var '%s=%s' " % (var, self._terraform_vars[var])

        init_command = "%s init %s" % (terraform_bin_command, terraform_commnand_args)
        terraform_command = "%s apply %s -auto-approve" % (terraform_bin_command, terraform_commnand_args)

        self._execute = [init_command, terraform_command]

        self.execute_shell_task()

        os.chdir(working_dir)
        shutil.rmtree('terraform_task')

    def pre_task(self):
        pass

    def post_task(self):
        pass

    @abc.abstractmethod
    def validator(self):
        raise NotImplementedError("Please Implement this method")
