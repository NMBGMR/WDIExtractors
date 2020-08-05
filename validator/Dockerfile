FROM clowder/pyclowder-python3:onbuild

ARG clowder_url

ENV RABBITMQ_URI=amqp://guest:guest@${clowder_url}
ENV MAIN_SCRIPT="validator.py"
