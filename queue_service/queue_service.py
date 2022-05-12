import time
import sys
import queue_service.constants as const
import os
import pprint as pp
from service_base.service_base import ServiceBase
from queue_service import conf, get_time
from queue_service.rqueue import Rqueue
from queue_service.utils import queue_has_beat
from service_base.connection import ResourceLockerConnection
import threading

rlocker = ResourceLockerConnection()


class QueueService(ServiceBase):
    def __init__(self):
        self.initializing_queues = rlocker.get_queues(status=const.STATUS_INITIALIZING)
        # We should fill this right after we checked what queues are not put in status pending
        self.pending_queues = None

    def run(self):
        """
        Actions to run when the service gets initialized
        :return None:
        """
        self.put_queues_on_pending()
        self.pending_queues = rlocker.get_queues(status=const.STATUS_PENDING)
        self.instantiate_pending_queue_objects()
        Rqueue.group_all()
        pp.pprint(Rqueue.grouped_queues)
        for group in Rqueue.grouped_queues:
            if group.get("group_type") == "label":
                resources = rlocker.get_lockable_resources(
                    free_only=True, label_matches=group.get("group_name")
                )
            elif group.get("group_type") == "name":
                resources = rlocker.get_lockable_resources(
                    free_only=True, name=group.get("group_name")
                )
            else:
                raise Exception("Group type should be either name or label!")

            if resources:
                group_queues = group.get("queues")
                next_queue = group_queues.pop(0)
                next_resource = resources.pop(0)
                # Prepare actions before entering to the thread:
                # The convention for the thread name,
                # please do not change unless decided to change naming convention

                thread_name = f"Thread-{next_queue.id}"
                alive_threads_names = list(map(lambda t: t.name, threading.enumerate()))
                is_thread_already_fired = thread_name in alive_threads_names
                if not is_thread_already_fired:
                    t = threading.Thread(
                        name=thread_name,
                        target=self.queue_beat_check,
                        args=(next_queue, next_resource)
                    )
                    t.start()

        return None

    def put_queues_on_pending(self):
        """
        This is a method that needs to be started with the startup of the svc.
        Reason: When the service starts, it needs to grab all the INITIALIZING queues,
            and put them on pending state.
        This will we a sign that this svc is healthy, and also it is being handled.
        :return: None
        """
        for queue in self.initializing_queues:
            queue_id = queue.get("id")
            queue_response = rlocker.change_queue(queue_id, status="PENDING")
            # For each queue, we should verify that the queues changed to being PENDING:
            is_queue_changed = (
                dict(queue_response.json()).get("status") == const.STATUS_PENDING
            )
            if not is_queue_changed:
                print(
                    f"There was a problem changing queue with ID {queue_id} to {const.STATUS_PENDING}! \n"
                    f"Unrecoverable error. Service Exiting..."
                )
                sys.exit()

        else:
            # All the checks did not enter if not is_queue_changed.
            # Therefore, service is ready to keep running.
            print(
                f"Service put all queues on {const.STATUS_PENDING} successfully! \n"
                if len(self.initializing_queues) > 0
                else f"No queues to Initialize. \n"
            )
            print(
                f"Total Queues that are {const.STATUS_PENDING}: "
                f"{len(rlocker.get_queues(status=const.STATUS_PENDING))}"
            )

        return None

    def instantiate_pending_queue_objects(self):
        """
        A method to instantiate objects so it will be easier
            to manipulate and filter specific data with the pending queues.
        We do not want to send requests, once we have the necessary info
            from get_queues()
        :return None:
        """
        for queue in self.pending_queues:
            # We want to initialize an instance of Rqueue only in case it is not already exists
            # as Rqueue instance:
            Rqueue(
                id=queue.get("id"),
                priority=queue.get("priority"),
                data=queue.get("data"),
            )

        return None

    def queue_beat_check(self, next_queue, next_resource):
        if queue_has_beat(
                queue_id=next_queue.id,
                in_last_x_seconds=conf["svc"].get("QUEUE_BEAT_TIMEOUT"),
        ):
            attempt_lock = rlocker.lock_resource(
                next_resource,
                signoff=next_queue.data.get("signoff"),
                link=next_queue.data.get("link"),
            )
            print(attempt_lock.json())

            # If attempt to lock was successful:
            if attempt_lock.json().get("is_locked"):
                rlocker.change_queue(
                    next_queue.id,
                    status=const.STATUS_FINISHED,
                    final_resource=next_resource.get('name'),
                )
            else:
                rlocker.change_queue(
                    next_queue.id,
                    status=const.STATUS_FAILED,
                    description=attempt_lock.text[:2048],
                )
        else:
            rlocker.abort_queue(
                next_queue.id,
                abort_msg=f"This queue was an orphan queue! \n"
                f"There was no associated client, because queue was not beating "
                f' in the last {conf["svc"].get("QUEUE_BEAT_TIMEOUT")} seconds',
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        We use context manager to describe what the service should to as post
            actions, after each beat.

        Actions:
             - Sleep the program to rest few seconds before next beat
             - Use clean console screen in order to display the most updated
                  status of all queues.
             - Clear the variables content that are indexed each beat, so that we
                will not have duplicated data on those variables
             - Write to status.log current timestamp,
                this will indicate the last time svc is healthy

        Handling Exception parameters that we need to receive:
        :param exc_type:
        :param exc_val:
        :param exc_tb:

        :return: None
        """

        time.sleep(conf["svc"].get("INTERVAL"))
        os.system("cls") if os.name == "nt" else os.system("clear")
        Rqueue.all.clear()
        Rqueue.grouped_queues.clear()
        with open(const.STATUS_LOGS_FILE, "a") as f:
            # For any new info to write, use comma-separation
            # Please keep \n as the first log to be written
            f.write(f"\nTIMESTAMP:{get_time().timestamp()},")
