from typing import Callable

from aiohttp import web

from udata_hydra.routes.checks import (
    create_check,
    get_all_checks,
    get_checks_aggregate,
    get_latest_check,
)
from udata_hydra.routes.resources import (
    create_resource,
    delete_resource,
    get_resource,
    get_resource_status,
    update_resource,
)
from udata_hydra.routes.resources_legacy import (
    create_resource_legacy,
    delete_resource_legacy,
    get_resource_legacy,
    update_resource_legacy,
)
from udata_hydra.routes.status import get_crawler_status, get_health, get_stats, get_worker_status


def generate_routes(
    routes_params: list[tuple[Callable, str, Callable, str | None]],
) -> list[web.RouteDef]:
    """
    Generate an aiohttp routes list of web.RouteDef objects from the given route parameters, with each of them having a variant with a trailing slash and one without, since aiohttp does not handle optional trailing slashes easily.
    Args:
        routes_params:
            A list of tuples, where each tuple contains:
                - method: The HTTP method (e.g., web.get, web.post).
                - path: The route path as a string.
                - handler: The handler function for the route.
                - name: An optional name for the route which can later be used by request.app.router to get the route.
    """
    routes: list[web.RouteDef] = []
    for method, path, handler, name in routes_params:
        routes.append(method(path, handler, name=name))
        if path.endswith("/"):
            routes.append(method(path[:-1], handler))
        else:
            routes.append(method(path + "/", handler))
    return routes


# Define the routes parameters
routes_params = [
    (web.get, "/api/checks/latest", get_latest_check, "get-latest-check"),
    (web.get, "/api/checks/all", get_all_checks, None),
    (web.get, "/api/checks/aggregate", get_checks_aggregate, None),
    (web.post, "/api/checks", create_check, None),
    # Routes for resources
    (web.get, "/api/resources/{resource_id}", get_resource, None),
    (web.get, "/api/resources/{resource_id}/status", get_resource_status, None),
    (web.post, "/api/resources", create_resource, None),
    (web.put, "/api/resources/{resource_id}", update_resource, None),
    (web.delete, "/api/resources/{resource_id}", delete_resource, None),
    # Routes for statuses
    (web.get, "/api/status/crawler", get_crawler_status, None),
    (web.get, "/api/status/worker", get_worker_status, None),
    (web.get, "/api/stats", get_stats, None),
    (web.get, "/api/health", get_health, None),
]

# Generate the routes
routes: list[web.RouteDef] = generate_routes(routes_params)

# TODO: legacy, to remove
legacy_routes_params = [
    (web.get, "/api/resources", get_resource_legacy, None),
    (web.post, "/api/resource/created", create_resource_legacy, None),
    (web.post, "/api/resource/updated", update_resource_legacy, None),
    (web.post, "/api/resource/deleted", delete_resource_legacy, None),
]
legacy_routes: list[web.RouteDef] = generate_routes(legacy_routes_params)
routes.extend(legacy_routes)
