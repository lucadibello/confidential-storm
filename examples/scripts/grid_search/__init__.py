"""grid_search - internals for the multi-node Storm grid-search benchmark.

The single CLI entry point is examples/scripts/run-grid-search.py.  Modules in
this package are split by concern: hosts, paths, config rendering, cluster
management, remote artifact collection, profiler CSV iteration, archival, and
the per-run driver.
"""
