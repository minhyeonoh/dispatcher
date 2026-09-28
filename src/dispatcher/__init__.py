"""Experiment dispatcher — schedules containerized trials across a
host fleet.

A research repo submits an *attempt*: a task list plus a container
spec describing how to run one trial. The dispatcher owns host
selection, concurrency caps (global / pool / host / attempt),
dispatch, failure detection, and state persistence. What a trial
computes — and what its outcome means — stays on the research-repo
side; the only contract is the outcome envelope
(`dispatcher.models.Outcome`) and the container labels
(`dispatcher.labels`).
"""
