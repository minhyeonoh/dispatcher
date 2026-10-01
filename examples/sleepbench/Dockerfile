# The ENVIRONMENT image: interpreter + dispatcher_sdk, no
# experiment code. Rebuilt when dependencies change, not per
# submission — hosts end up holding a handful of env images
# instead of one per sweep. The worker arrives at run time from
# the frozen source archive, unpacked by dispatcher_sdk.bootstrap.
FROM python:3.12-slim

# pandas is here for the READOUT side, not the work: `job.df` in
# readouts.py wants it. The SDK itself stays stdlib-only, so an
# image without pandas still works — it just uses job.columns.
RUN pip install --no-cache-dir "pandas>=2"

COPY dispatcher_sdk /opt/sdk/dispatcher_sdk
ENV PYTHONPATH=/opt/sdk
ENV PYTHONUNBUFFERED=1
