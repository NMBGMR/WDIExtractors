FROM clowder/pyclowder-python3:onbuild
RUN pip install ckanapi

ENV RABBITMQ_URI=amqp://guest:guest@clowder_rabbitmq_1
ENV MAIN_SCRIPT="wdi_extractor.py"