# The ENVIRONMENT image: interpreter + dispatcher_sdk, no
# experiment code. Rebuilt when dependencies change, not per
# submission — hosts end up holding a handful of env images
# instead of one per sweep. The worker arrives at run time from
# the frozen source archive, unpacked by dispatcher_sdk.bootstrap.
FROM python:3.12-slim

COPY dispatcher_sdk /opt/sdk/dispatcher_sdk
ENV PYTHONPATH=/opt/sdk
ENV PYTHONUNBUFFERED=1
