"""Private Python package, delivered by saltutil.sync_utils."""


def new_context(config, inventory, minion_id, grains, salt):
    """Salt entry point: return an ordinary Python operation context."""
    from .context import Context

    return Context(config, inventory, minion_id, grains, salt)


def library():
    """Expose ordinary Python modules across the Salt utility-loader boundary."""
    from types import SimpleNamespace
    from . import admin, cluster, config, journal, lifecycle, rpk, security, storage

    return SimpleNamespace(
        new_context=new_context,
        admin=admin,
        cluster=cluster,
        config=config,
        journal=journal,
        lifecycle=lifecycle,
        rpk=rpk,
        security=security,
        storage=storage,
    )
