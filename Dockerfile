FROM registry.insight-centre.org/sit/mps/docker-images/base-services@sha256:1cc57610ede299e3a30846ee4b24da6ae0b29403d2ae2542fcf6d7d623055a6a

## install only the service requirements
ADD ./Pipfile /service/Pipfile
ADD ./setup.py /service/setup.py
RUN mkdir -p /service/adaptation_planner/ && \
    touch /service/adaptation_planner/__init__.py
WORKDIR /service
RUN rm -f Pipfile.lock && pipenv lock -vvv && pipenv --rm && \
    pipenv install --system  && \
    rm -rf /tmp/pip* /root/.cache

## add all the rest of the code and install the actual package
## this should keep the cached layer above if no change to the pipfile or setup.py was done.
ADD . /service
RUN pip install -e . && \
    rm -rf /tmp/pip* /root/.cache
