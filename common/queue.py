import qmanager.q
import connexion
from .config import MONGODB_CONFIG


def queue(tag, deadline=0, repeat=None, service="api_tasks"):
    """
    Creates and returns a task queue for asynchronous code execution

    Parameters:
    - **tag** (*str*): A tag to identify and group tasks
    - **deadline** (*int*, default is 0): Time in seconds until the task deadline


    Description:
    This function attempts to retrieve the current request's URL and task tag from the request headers using `connexion.request`
    If the task tag is not found in the headers, the provided `tag` parameter is used
    It creates a `QueueJob` object with the specified parameters and returns a task queue


    Returns:
    - A task queue object, configured based on the provided parameters and the current request context

    Note:
    - **This function should only be used from this module*
    - The function is designed to manage asynchronous task execution and integrates with MongoDB through the `MONGODB_CONFIG`
    """

    url = ""
    try:
        url = str(connexion.request.url)
        tag = connexion.request.headers.get("x-queue-tag", tag)
    except:
        pass
    return qmanager.q.QueueJob(
        MONGODB_CONFIG['host'], tag=f"{service}/{tag}", url=url, deadline=deadline, repeat=repeat
    ).get_queue()
