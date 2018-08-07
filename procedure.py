#!/usr/env/bin python

#  Author: Yarin Galmor


import threading
import time
from prettytable import PrettyTable
from task import TaskStatus
from task_common_methods import configure_logger


class Procedure(object):
    def __init__(self, name, tasks=None):
        self._name = name
        self._tasks = tasks
        self._main_tasks = tasks
        self._is_procedure_finished_successfully = True
        self._is_procedure_stopped = False
        self._tasks_by_order = []
        self._currently_running_tasks = set()
        self._duration = 0
        self._starting_time = None
        self._summary_interval = 10

        self.logger = configure_logger(name.replace(" ", "_"))

    def get_task_by_order(self):
        return self._tasks_by_order

    def _wait_for_blocking_tasks(self, task):
        self.logger.info("Enter wait for task's blocking tasks - %s" % task.name)
        blocking_tasks = task.blocking_tasks
        task.logger.info("task name %s - task blocking tasks - %s" % (task.name,
                                                                      [blocking_task.name for blocking_task in blocking_tasks]))
        done_tasks = set()

        while blocking_tasks:
            for blocking_task in blocking_tasks:
                if blocking_task not in self._tasks:
                    raise RuntimeError("Blocking task  %s is not configured for this procedure." % blocking_task.name)
                blocking_task_name = blocking_task.name
                blocking_task_status = blocking_task.status
                task.logger.info("blocking task name %s - blocking task status - %s" % (blocking_task_name,
                                                                                        blocking_task_status.name))
                if blocking_task_status in (TaskStatus.DONE, TaskStatus.SKIPPED, TaskStatus.IGNORE_ERRORS):
                    task.logger.info("blocking task name %s - not running any more - %s" % (blocking_task_name,
                                                                                            blocking_task.status.name))
                    done_tasks.add(blocking_task)
                elif blocking_task_status.value < 0:
                    if blocking_task_status.name == 'FAILED' and blocking_task.retries_left:
                        continue
                    task.cancel()
                    raise RuntimeError("Blocking task %s has failed or have been canceled." % blocking_task.name)

            blocking_tasks = blocking_tasks.difference(done_tasks)
            time.sleep(5)

    def _wait_for_task(self, task):
        self.logger.info("Enter wait for task - %s" % task.name)
        while task.status.name == 'RUNNING' and task.duration < task.get_timeout():
            time.sleep(1)
            task.increment_duration()
            task.logger.info("Task duration - %s - %s" % (task.duration, task.name))
            task.logger.info("Task status - %s - %s" % (task.status.name, task.name))

        if task.duration >= task.get_timeout():
            task.set_task_status(TaskStatus.TIMEOUT_EXCEEDED)
            task.logger.error("Task exceeded its timeout.")
            raise RuntimeError()

        if task.status.name == 'FAILED':
            if task.ignore_task_errors:
                task.set_task_status(TaskStatus.IGNORE_ERRORS)
            else:
                raise RuntimeError()

    def summarize_procedure(self):
        self._update_duration()
        table = PrettyTable(["Name", "Duration", "Status"])
        for task in self._tasks_by_order:
            table.add_row([task.name, task.duration, task.status.name])
        self.logger.info("Procedure Summary:\n%s" % table.get_string())

    def _gather_tasks(self, tasks):
        for task in tasks:
            task.initiate_sub_tasks()
            current_sub_tasks = task.get_sub_tasks()
            if current_sub_tasks:
                self._tasks = self._tasks.union(self._gather_tasks(current_sub_tasks))

        self._tasks = self._tasks.union(tasks)
        return self._tasks

    def stop_procedure(self):
        """
        Stopping procedure by canceling all of its tasks.
        This method is taking place when procedure catch an exception of a certain task.
        """
        for task in self._tasks:
            if task not in self._tasks_by_order:
                #  If task wasn't started, it wasn't added to tasks_by_order list.
                self._tasks_by_order.append(task)
            if task.status.name == 'WAITING':
                #  If the task was not executed yet, stopping procedure would demand it to be canceled.
                task.cancel()

        self._is_procedure_stopped = True
        self._update_duration()

    def _start_task(self, task):
        try:
            self._wait_for_blocking_tasks(task)

            if task.skip_task:
                self.logger.info("Skipping task %s." % task.name)
                task.set_task_status(TaskStatus.SKIPPED)
                self._tasks_by_order.append(task)
            else:
                task.promote_task_status()
                task.set_starting_time(time.time())

                if task not in self._tasks_by_order:
                    self._tasks_by_order.append(task)

                task.validate_parameters()

                task.pre_task()

                self._currently_running_tasks.add(task)

                self.logger.info("Starting task - %s" % task.name)

                thread = threading.Thread(name=task.name, target=task.execute_task)
                thread.setDaemon(True)
                thread.start()

                self._wait_for_task(task)

                self._currently_running_tasks.remove(task)

                task.post_task()

                task.validator()

                if task.status.name == 'FINISHED':
                    task.promote_task_status()

        except Exception as e:
            self.logger.info("Exception was thrown by task - %s\n%s" % (task.name, e))
            if task.status not in (TaskStatus.TIMEOUT_EXCEEDED, TaskStatus.CANCELED):
                task.set_task_status(TaskStatus.FAILED)

            if task.retries_left and task.status == TaskStatus.FAILED:
                task.set_task_status(TaskStatus.WAITING)
                task.set_task_retries_left(task.retries_left - 1)
                task.set_starting_time(time.time())
                task.set_task_duration(0)
                self.logger.info("Re-run failed task - %s. Retries left - %s" % (task.name,
                                                                                 task.retries_left))
                self._start_task(task)
            else:
                if task.ignore_task_errors:
                    self.logger.info("Task %s failed but marked as IGNORE ERRORS. Continue procedure." % task.name)
                    task.set_task_status(TaskStatus.IGNORE_ERRORS)
                else:
                    self.stop_procedure()

    def _wait_for_tasks(self):
        running_tasks = self._tasks
        interval_starting_time = time.time()
        done_tasks = set()

        while running_tasks:
            for task in running_tasks:
                if task.status.value < 0 or task.status in (TaskStatus.DONE,
                                                            TaskStatus.SKIPPED,
                                                            TaskStatus.IGNORE_ERRORS):
                    if task.status == TaskStatus.FAILED and task.retries_left > 0:
                        self.logger.info("Task name - %s, Task status - %s, Task retries left - %s" % (task.name,
                                                                                                       task.status,
                                                                                                       task.retries_left))
                        continue
                    else:
                        done_tasks.add(task)
                        self.logger.info("Task %s is not running any more" % task.name)

            running_tasks = running_tasks.difference(done_tasks)

            if int(time.time() - interval_starting_time) > self._summary_interval:
                self.summarize_procedure()
                interval_starting_time = time.time()
            self._update_duration()

        for task in done_tasks:
            if task.status.value < 0:
                self._is_procedure_finished_successfully = False

        self.logger.info("All tasks and threads are finished")

    def _update_duration(self):
        self._duration = time.time() - self._starting_time

    def _set_blocking_tasks(self, task):
        if task.blocking_tasks:
            for sub_task in task.get_sub_tasks():
                sub_task.add_blocking_tasks(task.blocking_tasks)
                self._set_blocking_tasks(sub_task)

        task.add_blocking_tasks(task.get_sub_tasks())

    def start_procedure(self):
        self.logger.info("Starting Procedure with the following tasks - %s" % [task.name for task in self._tasks])

        self._gather_tasks(self._tasks)

        self.logger.info("Those are %s tasks for procedure %s:\n" % (len(self._tasks), self._name))
        tasks_table = PrettyTable(["Name"])
        for task in self._main_tasks:
            # Set each tasks blocking tasks. This loop is separated from the next loop because blocking tasks are
            # set recursively and not individual
            self._set_blocking_tasks(task)

        self._starting_time = time.time()

        for task in self._tasks:
            task.promote_task_status()
            thread = threading.Thread(target=self._start_task, args=(task,), name=task.name)
            thread.setDaemon(True)
            thread.start()
            tasks_table.add_row([task.name])

        self.logger.info("\n%s" % tasks_table.get_string())

        self.logger.info("Waiting for all tasks - %s" % [running_task.name for running_task in self._tasks])
        self._wait_for_tasks()

        self.summarize_procedure()

        if not self._is_procedure_finished_successfully:
            raise RuntimeError("Procedure failed")


