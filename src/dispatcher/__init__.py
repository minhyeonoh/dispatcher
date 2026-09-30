"""Experiment dispatcher — schedules containerized instances across a
host fleet.

A research repo submits an *job*: a task list plus a container
spec describing how to run one instance. The dispatcher owns host
selection, concurrency caps (global / pool / host / job),
dispatch, failure detection, and state persistence. What an instance
computes — and what its outcome means — stays on the research-repo
side; the only contract is the outcome envelope
(`dispatcher.core.models.Outcome`) and the container labels
(`dispatcher.core.labels`).
"""
