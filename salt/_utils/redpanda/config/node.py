"""Detect initialized brokers and produce validated configuration."""

import os
from . import render, validation


def initialized(ctx):
    return os.path.isdir(ctx.config['data_directory'] + '/redpanda/controller')


def configuration(ctx):
    validation.validate(ctx)
    return render.build(ctx.config, ctx.minion_id, ctx.inventory)
